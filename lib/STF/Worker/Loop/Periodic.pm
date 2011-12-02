package STF::Worker::Loop::Periodic;
use strict;
use parent qw( STF::Worker::Loop );

sub new {
    my ($class, %args) = @_;
    my $self = $class->SUPER::new(
        interval            => 60,
        %args,
    );
    return $self;
}

sub work {
    my $self = shift;

    die "Interval is not specified" unless $self->interval;

    my $guard = $self->container->new_scope();
    while ( $self->should_loop ) {
        $self->incr_processed();
        my $perloop_scope = $self->container->new_scope();
        $self->work_once();

        if ( $self->should_loop ) {
            sleep $self->interval;
        }
    }
}

1;