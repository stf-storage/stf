package STF::API::Queue::Resque;
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
    $queue->size($func);
}

sub enqueue {
    my ($self, $func, $object_id) = @_;

    local $STF::Log::PREFIX = "Resque";

    if ( ! defined $object_id ) {
        croakf("No object_id given for %s", $func);
    }

    my $worker_class = ucfirst $func;
    $worker_class =~ s/_(\w)/uc $1/ge;
    $worker_class = "STF::Worker::${worker_class}::Proxy";

    $self->enqueue_first_available($func, $object_id, sub {
        my ($queue_name, $object_id) = @_;
        if (STF_DEBUG) {
            debugf(
                "INSERT %s for %s (%s) on %s",
                $object_id,
                $func,
                $worker_class,
                $queue_name
            );
        }
        my $resque = $self->get($queue_name);
        $resque->push( $func => {
            class => $worker_class,
            args  => [ $object_id ],
        });
    });
}

no Mouse;

1;
