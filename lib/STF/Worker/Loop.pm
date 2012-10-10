package STF::Worker::Loop;
use Mouse;
use Time::HiRes ();
use STF::Constants qw(STF_DEBUG);
use STF::Log;

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

has max_jobs_per_minute => (
    is => 'rw',
);

has max_works_per_child => (
    is => 'rw',
    default => 1_000
);

has counter_key => (
    is => 'rw',
    required => 1,
);

sub incr_processed {
    my $self = shift;
    $self->get('Memcached')->incr($self->counter_key);
    ++$self->{processed};
}

sub global_job_count {
    $_[0]->get('Memcached')->get($_[0]->counter_key);
}

sub should_loop {
    my $self = shift;
    return $self->{processed} < $self->max_works_per_child;
}

sub throttle {
    my $self = shift;
    my $max_jobs_per_minute = $self->max_jobs_per_minute;
    if (! $max_jobs_per_minute) {
        return;
    }

    my $current_job_count;
    my $count = 0;
    while ($max_jobs_per_minute < ($current_job_count = $self->global_job_count)) {
        $count++;
        my $wait = rand(5);
        if (STF_DEBUG) {
            if ($count == 1) {
                debugf("We apparently processed %d jobs the last minute (max = %d).", $current_job_count, $max_jobs_per_minute);
            }
            debugf("Inserting wait of %f", $wait);
        }
        Time::HiRes::sleep($wait);
    }
}

sub work {}

no Mouse;

1;
