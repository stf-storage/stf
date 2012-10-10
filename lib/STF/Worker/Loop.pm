package STF::Worker::Loop;
use Mouse;
use Time::HiRes ();
use STF::Constants qw(STF_DEBUG);
use STF::Log;

has parent => (
    is => 'ro',
    required => 1,
    handles => {
        name => 'name',
    }
);

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

has max_jobs_per_minute_key => (
    is => 'rw',
    default => sub { $_[0]->to_keyname("max_jobs_per_minute") }
);

has counter_key => (
    is => 'rw',
    default => sub { $_[0]->to_keyname("processed_jobs") }
);

has reload_key => (
    is => 'rw',
    default => sub { "stf.worker.reload" }
);

# XXX If in the future we have more states to check, we should
# make this a bit vector like we're doing in Drone.pm
has should_reload => (
    is => 'rw',
    default => 0,
);

has next_reload => (
    is => 'rw',
    default => 0,
);

has now => (
    is => 'rw'
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

sub to_keyname {
    my($self, $key, $prefix) = @_;
    $prefix ||= "stf.worker";
    return join ".", $prefix, $self->name, $key;
}

sub update_now {
    $_[0]->now(Time::HiRes::time());
}

sub check_state {
    my $self = shift;

    my $reload_key = $self->reload_key();
    my $memd = $self->get('Memcached');
    my $h = $memd->get_multi(
        $reload_key,
    );

    # XXX see caveat about bitmasks above
    $self->should_reload(0);

    my $now = $self->now;
    my $when_to_reload = $h->{$reload_key} || 0;
    if ($self->next_reload <= $now ||
        $when_to_reload >= $now
    ) {
        $self->should_reload(1);
    }
}

sub reload {
    my $self = shift;

    if (! $self->should_reload) {
        return;
    }

    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare(<<EOSQL);
        SELECT varvalue FROM config WHERE varname = ?
EOSQL
    my ($max_jobs_per_minute) = $dbh->selectrow_array($sth, undef, $self->max_jobs_per_minute_key);

    $self->max_jobs_per_minute($max_jobs_per_minute);
    $self->next_reload($self->now + 30);
}

sub throttle {
    my $self = shift;
    my $max_jobs_per_minute = $self->max_jobs_per_minute;
    if (! $max_jobs_per_minute) {
        return;
    }

    my $current_job_count = $self->global_job_count;
    if (STF_DEBUG) {
        if ($current_job_count > $max_jobs_per_minute) {
            debugf("Need to throttle!: Processed %d (max = %d).", $current_job_count, $max_jobs_per_minute);
        }
    }
    while ($max_jobs_per_minute > 0 && $max_jobs_per_minute < $current_job_count) {
        Time::HiRes::sleep(rand(5));
        $self->check_state;
        if ($self->reload) {
            $max_jobs_per_minute = $self->max_jobs_per_minute;
        }
        $current_job_count = $self->global_job_count;
    }
}

sub work {}

no Mouse;

1;
