package STF::API::Queue::Q4M;
use Mouse;
use Digest::MurmurHash ();
use STF::Constants qw(:func STF_DEBUG);
use STF::Log;

with 'STF::Trait::WithDBI';

has funcmap => (
    is => 'rw',
    lazy => 1,
    builder => 'build_funcmap'
);

has queue_names => (
    is => 'rw',
    required => 1,
);

sub build_funcmap {
    return {
        replicate     => Q4M_FUNC_REPLICATE,
        delete_object => Q4M_FUNC_DELETE_OBJECT,
        delete_bucket => Q4M_FUNC_DELETE_BUCKET,
        repair_object => Q4M_FUNC_REPAIR_OBJECT,
        object_health => Q4M_FUNC_OBJECT_HEALTH,
    }
}

sub get_func_id {
    my ($self, $func) = @_;

    $self->funcmap->{$func};
}

sub size {
    my ($self, $func) = @_;

    my $table = "queue_$func";
    my $total = 0;
    foreach my $queue_name ( @{ $self->queue_names } ) {
        my $dbh = $self->dbh($queue_name);
        my ($count) = $dbh->selectrow_array( <<EOSQL );
            SELECT COUNT(*) FROM $table
EOSQL
        $total += $count;
    }
    return $total;
}

sub enqueue {
    my ($self, $func, $object_id) = @_;

    local $STF::Log::PREFIX = "Q4M";
    my $func_id = $self->get_func_id( $func );
    if (! $func_id ) {
        croakf("PANIC: Don't know what the function ID for %s is", $func);
    }

    if ( ! defined $object_id ) {
        croakf("No object_id given for %s", $func);
    }

    my $table = "queue_$func";
    # Sort the queue names by the murmur hash value of queue_name + object_id
    my $queue_names = $self->queue_names;
    my %queues = (
        map {
            ( $_ => Digest::MurmurHash::murmur_hash( $_ . $object_id ) )
        } @$queue_names
    );
    foreach my $queue_name ( sort { $queues{$a} <=> $queues{$b} } keys %queues) {
        my $dbh = $self->dbh($queue_name);
        debugf(
            "INSERT %s into %s for %s on %s",
            $object_id,
            $table,
            $func,
            $queue_name
        );

        my $rv;
        my $err = STF::Utils::timeout_call(
            0.5,
            sub {
                $rv = $dbh->do(<<EOSQL, undef, $object_id );
                    INSERT INTO $table ( args, created_at ) VALUES (?, UNIX_TIMESTAMP( NOW() ) )
EOSQL
            }
        );
        if ( $err ) {
            # XXX Don't wrap in STF_DEBUG
            critf("Error while enqueuing: %s", $err);
            critf(" + func: %s", $func);
            critf(" + object ID = %s", $object_id);
            next;
        }

        return $rv;
    }

    return ();
}

no Mouse;

1;
