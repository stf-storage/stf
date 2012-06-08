package STF::API::Queue::Schwartz;
use Mouse;
use STF::Constants qw(STF_DEBUG);
use TheSchwartz;

with qw(STF::Trait::WithDBI);

has ability_map => (
    is => 'rw',
    lazy => 1,
    builder => 'build_ability_map',
);

sub build_ability_map {
    return {
        replicate     => "STF::Worker::Replicate::Proxy",
        delete_object => "STF::Worker::DeleteObject::Proxy",
        delete_bucket => "STF::Worker::DeleteBucket::Proxy",
        repair_object => "STF::Worker::RepairObject::Proxy",
        object_health => "STF::Worker::ObjectHealth::Proxy",
    };
}

sub size {
    my ($self, $func) = @_;

    my $dbh = $self->get('DB::Queue');
    my $client = $self->get_client();
    my $ability = $self->get_ability($func);
    my ($count) = $dbh->selectrow_array( <<EOSQL, undef, $ability );
        SELECT COUNT(*) FROM
            job j JOIN funcmap f ON j.funcid = f.funcid
            WHERE f.funcname = ?
EOSQL
    return $count;
}

sub get_ability {
    my ($self, $func) = @_;

    $self->ability_map->{$func};
}

sub get_client {
    my ($self) = @_;

    my $client = $self->{client};
    if (! $client) {
        my $dbh = $self->get('DB::Queue') or
            Carp::confess( "Could not fetch DB::Queue" );
        my $driver = Data::ObjectDriver::Driver::DBI->new( dbh => $dbh );
        $self->{client} = $client = TheSchwartz->new( databases => [ { driver => $driver } ] );
    }
    return $client;
}

sub enqueue {
    my ($self, $func, $object_id) = @_;

    my $ability = $self->get_ability($func);
    if (! $ability ) {
        Carp::confess( "PANIC: Don't know what the schwartz ability for $func is" );
    }

    if ( ! defined $object_id ) {
        Carp::confess("No object_id given for $func");
    }

    my $client = $self->get_client();

    my $rv;
    my $err = STF::Utils::timeout_call(
        0.5,
        sub {
            $rv = $client->insert( $ability, $object_id );
        }
    );
    if ( $err ) {
        # XXX Don't wrap in STF_DEBUG
        printf STDERR "[     Queue] Error while enqueuing: %s\n + func: %s\n + object ID = %s\n",
            $err,
            $func,
            $object_id,
        ;
        next;
    }

    if ( STF_DEBUG ) {
        print STDERR "[     Queue] Engqueued $ability ($object_id)\n";
    }
    return $rv;
}

no Mouse;

1;

