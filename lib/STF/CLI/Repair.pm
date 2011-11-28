package STF::CLI::Repair;
use strict;
use parent qw(STF::CLI::Base);
use Parallel::ForkManager;
use STF::Constants qw(STF_DEBUG);

sub opt_specs {
    (
        'object_id|o=s',
        'storage_id|s=s',
        'logical|L!',
        'physical|P!'
    );
}

sub run {
    my $self = shift;

    if ($self->options->{object_id}) {
        $self->repair_object();
    } elsif ($self->options->{storage_id}) {
        $self->repair_storage();
    } else {
        die "You must specify object_id or storage_id";
    }
}

sub repair_object {
    my $self = shift;
    my $options = $self->options;
    $self->get( 'API::Queue' )->enqueue( repair_object => $options->{object_id} );
}

sub repair_storage {
    my $self = shift;
    my $options = $self->options;
    if ($options->{physical}) {
        $self->find_physical_problems();
    } else {
        $self->find_logical_problems();
    }
}

sub find_physical_problems {
    my $self = shift;
    my $options = $self->options;
    my $dbh = $self->get( 'DB::Master' );
    my $pfm = Parallel::ForkManager->new(10);

    # find objects in storage X, and then check for validity
    my $sth = $dbh->prepare( <<EOSQL );
        SELECT DISTINCT o.id
            FROM object o JOIN entity e ON o.id = e.object_id
            WHERE e.storage_id = ?
            AND o.id
            LIMIT 1000 OFFSET ?
EOSQL

    my $url_sth = $dbh->prepare( <<EOSQL );
        SELECT CONCAT_WS('/', s.uri, o.internal_name) as url
            FROM entity e INNER JOIN storage s ON e.storage_id = s.id
                          INNER JOIN object  o ON e.object_id  = o.id
            WHERE o.id = ?
EOSQL

    my $offset = 0;
    while ( $sth->execute( $options->{storage_id}, $offset ) > 0 ) {
        $offset += 1000;

        my $objects = $sth->fetchall_arrayref([0]);

        $pfm->start and next;
        eval { 
            my $furl = $self->get('Furl');
            my $queue_api = $self->get('API::Queue');
            foreach my $object_id ( @$objects ) {
                $url_sth->execute( $object_id );
                my $url;
                $url_sth->bind_columns( \($url) );

                my $repair = 0;
                while ( $url_sth->fetchrow_arrayref ) {
                    my (undef, $code) = $furl->head( $url );
                    if ( ! HTTP::Status::is_success( $code ) ) {
                        $repair++;
                        last;
                    }
                }

                if ( $repair ) {
                    $queue_api->enqueue( repair_object => $object_id );
                }
            }
        };
        warn $@ if $@;

        $pfm->finish;
    }

    $pfm->wait_all_children;
}

sub find_logical_problems {
    my $self = shift;
    my $options = $self->options;
    my $dbh = $self->get( 'DB::Master' );

    my $queue_api = $self->get( 'API::Queue' );
    my $count_sth = $dbh->prepare( <<EOSQL );
        SELECT COUNT(*) FROM entity WHERE object_id = ?
EOSQL

    my $sth = $dbh->prepare( <<EOSQL );
        SELECT DISTINCT o.id, o.num_replica
            FROM object o JOIN entity e ON o.id = e.object_id
            WHERE e.storage_id = ?
EOSQL

    $sth->execute( $options->{storage_id} );
    my ($object_id, $num_replica);
    $sth->bind_columns( \($object_id, $num_replica) );

    while ( $sth->fetchrow_arrayref ) {
        # find the number of replicas
        $count_sth->execute( $object_id );
        my ($replicas) = $count_sth->fetchrow_array();
        if ( $replicas < $num_replica ) {
            if ( STF_DEBUG ) {
                print STDERR "[Repairing] $object_id\n";
            }
            $queue_api->enqueue( repair_object => $object_id );
        }
    }
}

1;
