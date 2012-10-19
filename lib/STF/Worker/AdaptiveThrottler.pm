package STF::Worker::AdaptiveThrottler;
use Mouse;
use STF::Constants qw(:storage STF_DEBUG);
use STF::Log;

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

has '+interval' => (
    default => 60 * 1_000_000
);

# Note: threshold from SNMP is NOT in fractional. use 1=100
has la_threshold => (
    is => 'ro',
    default => 700
);

before work => sub {
    my $self = shift;

    # before starting work, we would like to start the stats collector
    $self->get('API::Config')->set(
        "stf.drone.StatsCollector.instances" => 1
    );

    my $now = time();
    $self->get('Memcached')->set_multi(
        map { [ "stf.drone.$_", $now ] } qw(election reload balance),
    );
};

sub work_once {
    my $self = shift;

    eval {
        local $SIG{TERM} = sub { die "Received Signal" };

        my $is_high = $self->check_loads();
        my @workers = qw(
            ContinuousRepair
            DeleteBucket
            DeleteObject
            RepairObject
            RepairStorage
            Replicate
            StorageHealth
        );
        foreach my $worker_name ( @workers ) {
            $self->set_throttle_limit($worker_name, $is_high);
        }
    };
    if ($@) {
        critf("Bailing out of worker: %s", $@);
        # Commit suicide
        kill TERM => $$;
    }
}

sub check_loads {
    my $self = shift;

    if (STF_DEBUG) {
        debugf("Checking loads for storages");
    }

    # Find the max load average in the whole system, and make sure
    # that it doesn't get over $la_threshold
    my @storages = $self->get("API::Storage")->search({
        mode => { IN => [
            STORAGE_CLUSTER_MODE_READ_ONLY,
            STORAGE_CLUSTER_MODE_READ_WRITE,
            STORAGE_MODE_REPAIR_NOW,
        ] }
    });

    my $la_threshold = $self->la_threshold;
    my $loadhigh = 0;

    my $time = time();
    my $t    = $time - $time % 60;
    my @keys = map { "storage.load.$_->{id}.$t" } @storages;
    my $h    = $self->get('Memcached')->get_multi(@keys);

    foreach my $key (keys %$h) {
        my $load    = $h->{$key};
        if (! $load) {
            next;
        }
        my $loadavg = $load->[0];
        if (STF_DEBUG) {
            debugf(" + Load average for %s is %f", ($key =~ /^storage\.load\.([^\.]+)/), $loadavg / 100);
        }

        if ($loadavg > $la_threshold) { 
            if (STF_DEBUG) {
                debugf("   Load average too high!");
            }
            $loadhigh++;
        }
    }

    return $loadhigh;
}

sub set_throttle_limit {
    my ($self, $worker_name, $is_high) = @_;

    my $config_api = $self->get('API::Config');

    my $auto_adapt = $config_api->load_variable("stf.worker.$worker_name.throttle.auto_adapt");

    if ( ! $auto_adapt) {
        if (STF_DEBUG) {
            debugf("Auto-adapt is not enabled. Skipping worker %s", $worker_name);
        }
        return;
    }

    my $threshold_key = "stf.worker.$worker_name.throttle.current_threshold";
    my $max_threshold_key = "stf.worker.$worker_name.throttle.threshold";
    my $cur_threshold = $config_api->load_variable($threshold_key) || 0;
    my $max_threshold = $config_api->load_variable($max_threshold_key) || 0;

    if ($is_high) { 
        # change the loadavg to 60%
        if ($max_threshold == 0) {
            my $default_threshold = 300;
            # XXX we have set the threshold at "unlimited". we should throttle
            # automatically. If in case the max threshold is not set, magically
            $config_api->set($max_threshold_key, $default_threshold);
            $max_threshold = $default_threshold;
            $cur_threshold = $max_threshold / 2;
        }

        my $new_threshold = int($cur_threshold * 0.6);
        if ($new_threshold < 0) {
            $new_threshold = 0;
        }

        $config_api->set($threshold_key, $new_threshold);
    } else {
        # otherwise, if our current threshold is less than the maximum
        # then increase over 10%.
        my ($new_threshold);
            
        if ($max_threshold > $cur_threshold) {
            if ($cur_threshold < 10) {
                $new_threshold = $cur_threshold + 5;
            } else {
                $new_threshold = int($cur_threshold * 1.1);
            }

            if ($max_threshold < $new_threshold) {
                $new_threshold = $max_threshold;
            }
        }

        if ($new_threshold) {
            $config_api->set($threshold_key, $new_threshold);
        }
    }
}

1;