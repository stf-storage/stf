package STF::Worker::Loop;
use Mouse;
use POSIX qw(SIGINT SIGQUIT SIGTERM);
use Time::HiRes ();
use STF::API::Throttler;
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

has throttle_threshold => (
    is => 'rw',
    default => 0,
);

has max_works_per_child => (
    is => 'rw',
    default => 1_000
);

has throttle_threshold_key => (
    is => 'rw',
    default => sub { $_[0]->to_keyname("throttle.current_threshold") }
);

has counter_key => (
    is => 'rw',
    default => sub { $_[0]->to_keyname("processed_jobs") }
);

has reload_key => (
    is => 'rw',
    default => sub { $_[0]->to_keyname("reload") }
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

has is_throttled => (
    is => 'rw',
    default => 0,
);

has next_check_throttle => (
    is => 'rw',
    default => 0
);

has next_check_state => (
    is => 'rw',
    default => 0
);

has now => (
    is => 'rw'
);

has throttler => (
    is => 'ro',
    lazy => 1,
    default => sub {
        my $self = shift;
        return STF::API::Throttler->new(
            key => $self->counter_key,
            threshold => 0, # initially unlimited
            container => $self->container,
        );
    }
);

sub incr_processed {
    my $self = shift;
    $self->throttler->incr($self->now);
    $self->processed($self->processed + 1);
}

sub global_job_count {
    my $self = shift;
    $self->throttler->current_count($self->now);
}

sub should_loop {
    my $self = shift;
    return $self->processed < $self->max_works_per_child;
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

    if ($self->next_check_state > $self->now) {
        return;
    }

    $self->next_check_state($self->now + 10);
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
        $when_to_reload > $now
    ) {
        $self->should_reload(1);
    }
}

sub reload {
    my $self = shift;

    if (! $self->should_reload) {
        return;
    }

    if (STF_DEBUG) {
        debugf("Reloading worker config");
    }
    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare(<<EOSQL);
        SELECT varvalue FROM config WHERE varname = ?
EOSQL
    my ($throttle_threshold) = $dbh->selectrow_array($sth, undef, $self->throttle_threshold_key);

    $self->throttler->threshold($throttle_threshold);
    $self->should_reload(0);
    $self->next_reload($self->now + 60);
}

sub check_throttle {
    my $self = shift;
    my $throttle_threshold = $self->throttler->threshold;
    if (! $throttle_threshold) {
        return;
    }

    if ($self->is_throttled) {
        # We're throttled. Forcefully sleep
        if (STF_DEBUG) {
            debugf("Throttled, sleeping...");
        }

        # We may receive a signal while we're sleeping. In that case
        # we just want to bail out of this check, so set a signal
        # handler just for that
        eval {
            my $sigset = POSIX::SigSet->new( SIGINT, SIGQUIT, SIGTERM );
            my $cancel = POSIX::SigAction->new(sub {
                die "Sleep Canceled";
            }, $sigset, &POSIX::SA_NOCLDSTOP);
            Time::HiRes::sleep(rand(10));
        };
        if ($@ =~ /Sleep Canceled/) {
            # Resend me the TERM signal! this will trigger the
            # signal handler from the calling loop
            kill TERM => $$;
            return;
        }

        # is our probation period over?
        if ($self->next_check_throttle > $self->now) {
            # nope, return 1 because we're still throttled
            return 1;
        }
    }

    $self->is_throttled(0);
    $self->next_check_throttle($self->now + rand($self->throttler->throttle_span));
    if (! $self->throttler->should_throttle($self->now)) {
        return;
    }
    $self->is_throttled(1);
    return 1;
}

sub work {}

no Mouse;

1;
