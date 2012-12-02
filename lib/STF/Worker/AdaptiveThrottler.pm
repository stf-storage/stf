package STF::Worker::AdaptiveThrottler;
use Mouse;
use STF::Constants qw(:storage STF_DEBUG);
use STF::Log;

use constant +{
    WEIGHT_10MIN_LOAD_AVG => 20,
    WEIGHT_05MIN_LOAD_AVG => 10,
    WEIGHT_01MIN_LOAD_AVG =>  1
};

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

has '+interval' => (
    default => 60 * 1_000_000
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
            STORAGE_MODE_READ_ONLY,
            STORAGE_MODE_READ_WRITE,
            STORAGE_MODE_REPAIR_NOW,
        ] }
    });

    my $la_threshold = $self->get('API::Config')->load_variable(
        "stf.worker.AdaptiveThrottler.loadavg_threshold"
    );
    if (! defined $la_threshold || $la_threshold <= 0) {
        $la_threshold = 650;
    }

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

        # First check the latest load average. If this is too high, 
        # immediately bail out and get ready to drop the threshold
        if ($load->[0] > $la_threshold) {
            if (STF_DEBUG) {
                debugf("    Load average too high! (1 min avg)");
            }
            $loadhigh++;
            next;
        }

        # Get the weighted mean of 1 min, 5 min, and 10 min avg
        # Basic idea: if the load average has been high for the last 10 min
        # but is lower this last minute, it may just mean that we had a very
        # short drop. So the loadavg for the longer duration is more
        # important than the most recent one
        #
        # ergo, we do:
        #   loadavg = (
        #       ( weight10 * 10min_avg ) +
        #       ( weight5 * 5min_avg ) +
        #       ( weight1 * 1min_avg ) 
        #   ) / (weight10 + weight5 + weight1)
        #
        # case in point:
        #   10min: 9.1 (threshold)
        #    5min: 6.8 (right below threshold)
        #    1min: 5.0 (well below threshold)
        # wehn weight10 = 50, weight5 = 10, and weight1 = 1
        #   loadvg = (355 + 68 + 5) / 61 = 7.016

        my $loadavg = (
            WEIGHT_01MIN_LOAD_AVG * $load->[0] +
            WEIGHT_05MIN_LOAD_AVG * $load->[1] +
            WEIGHT_10MIN_LOAD_AVG * $load->[2]
        ) / (WEIGHT_01MIN_LOAD_AVG + WEIGHT_05MIN_LOAD_AVG + WEIGHT_10MIN_LOAD_AVG);
        if (STF_DEBUG) {
            debugf(" + Load average for %s is %f", ($key =~ /^storage\.load\.([^\.]+)/), $loadavg / 100);
        }

        if ($loadavg > $la_threshold) { 
            if (STF_DEBUG) {
                debugf("   Load average too high! (weighted mean)");
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

        my $drop_rate = $config_api->load_variable("stf.worker.$worker_name.throttle.drop_rate");
        if (! defined $drop_rate || $drop_rate <= 0 || $drop_rate >= 1) {
            $drop_rate = 0.6;
        }
        my $new_threshold = int($cur_threshold * $drop_rate);
        if ($new_threshold < 0) {
            $new_threshold = 0;
        }

        $config_api->set($threshold_key, $new_threshold);
    } elsif ($max_threshold < $cur_threshold) {
        # If somebody changed the DB value to a lower one, reduce regardless
        # of what our current situation is
        $config_api->set($threshold_key, $max_threshold);
    } else {
        # otherwise, if our current threshold is less than the maximum
        # then increase over 10%.
        my ($new_threshold);
            
        if ($max_threshold > $cur_threshold) {
            my $increase_rate = $config_api->load_variable("stf.worker.$worker_name.throttle.increase_rate");
            if (!defined $increase_rate || $increase_rate <= 0) {
                $increase_rate = 0.06;
            }
            my $increment = int($cur_threshold * $increase_rate);
            if ($increment < 1) {
                $increment = 1;
            }
            $new_threshold += $cur_threshold + $increment;

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