package STF::API::Queue;
use Mouse::Role;
use Digest::MurmurHash ();
use STF::Constants qw(STF_DEBUG);
use STF::Log;
use STF::Utils ();

has queue_names => (
    is => 'rw',
    required => 1,
);

requires qw(
    enqueue
    size_for_queue
);

sub size_total {
    my ($self, $func) = @_;
    my $total = 0;
    foreach my $queue_name ( @{ $self->queue_names } ) {
        $total += $self->size_for_queue( $func, $queue_name );
    }
    return $total;
}

sub size {
    no warnings 'redefine';
    *size = \&size_total;
    goto \&size;
}

sub enqueue_first_available {
    my ($self, $func, $object_id, $cb) = @_;

    my $queue_names = $self->queue_names;
    my %queues = (
        map {
            ( $_ => Digest::MurmurHash::murmur_hash( $_ . $object_id ) )
        } @$queue_names
    );
    foreach my $queue_name ( sort { $queues{$a} <=> $queues{$b} } keys %queues) {
        if (STF_DEBUG) {
            debugf("Attempting to enqueue job into queue '%s'", $queue_name);
        }

        my $rv;
        my $err = STF::Utils::timeout_call( 0.5, sub {
            $rv = $cb->($queue_name, $object_id)
        });
        if ( $err ) {
            # XXX Don't wrap in STF_DEBUG
            critf("Error while enqueuing: %s\n + func: %s\n + object ID = %s\n",
                $err,
                $func,
                $object_id,
            );
            next;
        }

        return $rv;
    }

    return ();
}

no Mouse::Role;

1;