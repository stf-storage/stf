package STF::API::Queue::Redis;
use Mouse;
use STF::Constants qw(STF_DEBUG);
use STF::Log;

with qw(
    STF::API::Queue
    STF::Trait::WithContainer
);

sub size_for_queue {
    my ($self, $func, $queue_name) = @_;

    my $queue = $self->get($queue_name);
    $queue->llen($func);
}

sub enqueue {
    my ($self, $func, $object_id) = @_;

    local $STF::Log::PREFIX = "Redis";

    if ( ! defined $object_id ) {
        croakf("No object_id given for %s", $func);
    }

    my $encoder = $self->get('JSON');

    $self->enqueue_first_available($func, $object_id, sub {
        my ($queue_name, $object_id) = @_;
        if (STF_DEBUG) {
            debugf(
                "INSERT %s for %s on %s",
                $object_id,
                $func,
                $queue_name
            );
        }
        my $resque = $self->get($queue_name);
        $resque->rpush( $func => $encoder->encode({ args => [ $object_id ] }));
    });
}

no Mouse;

1;
