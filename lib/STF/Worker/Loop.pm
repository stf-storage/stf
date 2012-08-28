package STF::Worker::Loop;
use Mouse;

has interval => (
    is => 'rw',
    default => 1_000_000
);

has processed => (
    is => 'rw',
    default => 0,
);

has queue_name => (
    is => 'rw',
    lazy => 1,
    default => sub {
        my $queue_name =
            $ENV{STF_WORKER_QUEUE_NAME} ||
            'DB::Queue'
        ;
        return $queue_name;
    }
);

has max_works_per_child => (
    is => 'rw',
    default => 1_000
);

sub incr_processed {
    my $self = shift;
    ++$self->{processed};
}

sub should_loop {
    my $self = shift;
    return $self->{processed} < $self->max_works_per_child;
}

sub work {}

no Mouse;

1;
