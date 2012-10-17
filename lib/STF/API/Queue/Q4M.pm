package STF::API::Queue::Q4M;
use Mouse;
use Digest::MurmurHash ();
use STF::Constants qw(:func STF_DEBUG);
use STF::Log;

with qw(
    STF::Trait::WithDBI
    STF::API::Queue
);

has funcmap => (
    is => 'rw',
    lazy => 1,
    builder => 'build_funcmap'
);

sub build_funcmap {
    return {
        notify        => Q4M_FUNC_NOTIFY,
        replicate     => Q4M_FUNC_REPLICATE,
        delete_object => Q4M_FUNC_DELETE_OBJECT,
        delete_bucket => Q4M_FUNC_DELETE_BUCKET,
        repair_object => Q4M_FUNC_REPAIR_OBJECT,
    }
}

sub get_func_id {
    my ($self, $func) = @_;

    $self->funcmap->{$func};
}

sub size_for_queue {
    my ($self, $func, $queue_name) = @_;
    my $dbh = $self->dbh($queue_name);
    my $table = "queue_$func";
    my ($count) = $dbh->selectrow_array( <<EOSQL );
        SELECT COUNT(*) FROM $table
EOSQL
    return $count;
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

    $self->enqueue_first_available( $func, $object_id, sub {
        my ($queue_name, $object_id) = @_;
        my $dbh = $self->dbh($queue_name);
        if (STF_DEBUG) {
            debugf(
                "INSERT %s into %s for %s on %s",
                $object_id,
                $table,
                $func,
                $queue_name
            );
        }
        return $dbh->do(<<EOSQL, undef, $object_id );
            INSERT INTO $table ( args, created_at ) VALUES (?, UNIX_TIMESTAMP( NOW() ) )
EOSQL
    } );
}

no Mouse;

1;
