package STF::API::Queue::Q4M;
use strict;
use parent qw(STF::Trait::WithDBI);
use Digest::MurmurHash ();
use STF::Constants qw(:func STF_DEBUG);
use Class::Accessor::Lite
    new => 1,
    rw => [ qw(
        funcmap
        queue_names
    ) ]
;

sub get_func_id {
    my ($self, $func) = @_;

    my $funcmap = $self->funcmap;
    if (! $funcmap) {
        $self->funcmap( $funcmap = {
            replicate     => Q4M_FUNC_REPLICATE,
            delete_object => Q4M_FUNC_DELETE_OBJECT,
            delete_bucket => Q4M_FUNC_DELETE_BUCKET,
            repair_object => Q4M_FUNC_REPAIR_OBJECT,
            object_health => Q4M_FUNC_OBJECT_HEALTH,
        } );
    }

    $funcmap->{$func};
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
    my $func_id = $self->get_func_id( $func );
    if (! $func_id ) {
        Carp::confess( "PANIC: Don't know what the function ID for $func is" );
    }

    if ( ! defined $object_id ) {
        Carp::confess("No object_id given for $func");
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
        if (STF_DEBUG) {
            printf STDERR "[     Queue] INSERT %s into %s for %s on %s\n",
                $object_id, $table, $func, $queue_name
            ;
        }

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
            printf STDERR "[     Queue] Error while enqueuing: %s\n + func: %s\n + object ID = %s\n",
                $err,
                $func,
                $object_id,
            ;
            next;
        }

        return $rv;
    }
}

1;
