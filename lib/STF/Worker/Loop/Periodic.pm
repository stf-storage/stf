package STF::Worker::Loop::Periodic;
use Mouse;

extends 'STF::Worker::Loop';
with 'STF::Trait::WithContainer';

has '+interval' => (
    default => 60 * 1_000_000
);

sub work {
    my ($self, $impl) = @_;

    die "Interval is not specified" unless $self->interval;

    my $guard = $self->container->new_scope();
    while ( $self->should_loop ) {
        $self->incr_processed();
        my $perloop_scope = $impl->container->new_scope();
        $impl->work_once();

        if ( $self->should_loop ) {
            Time::HiRes::usleep($self->interval);
        }
    }
}

no Mouse;

1;