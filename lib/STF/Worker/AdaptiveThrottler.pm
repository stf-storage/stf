package STF::Worker::AdaptiveThrottler;
use Mouse;
use URI;
use Net::SNMP;
use STF::Constants qw(:storage STF_DEBUG);
use STF::Utils ();
use STF::Log;

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

has '+interval' => (
    default => 60 * 1_000_000
);

# Note: threshold from SNMP is NOT in fractional. use 1=100
has la_threshold => (
    is => 'ro',
    default => 500
);

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

sub create_snmp_session {
    my ($self, $host) = @_;

    # XXX cheat. Should this information be retrieved from DB?
    my $config = $self->get('config')->{SNMP} || {};
    return Net::SNMP->session(
        -timeout  => 5,
        -hostname => $host,
        %$config,
    );
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
    my $baseoid = ".1.3.6.1.4.1.2021.10.1.5"; # laLoadInt
    foreach my $storage ( @storages ) {
        # XXX extract host out of URI
        my $uri = URI->new($storage->{uri});
        my $host = $uri->host;
        if (STF_DEBUG) {
            debugf("Sending SNMP to %s", $host);
        }
        my ($session, $error) = $self->create_snmp_session($host);
        if ($error) {
            critf($error);
            next;
        }
        my $result = $session->get_table(
            -baseoid => $baseoid
        );
        my $loadavg = $result->{"$baseoid.1"};
        if (STF_DEBUG) {
            debugf(" + Load average for %s is %f", $host, $loadavg / 100);
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
    my @list = $config_api->search({
        varname => {
            'LIKE' => sprintf('stf.worker.%s.throttle.%%', $worker_name),
        }
    });
    return unless @list;
    my $h = +{ map { ($_->{varname}, $_->{varvalue}) } @list };
    foreach my $key (keys %$h) {
        my $new_key = $key;
        $new_key =~ s/^stf\.worker\.[^\.]+\.throttle\.//;
        $h->{$new_key} = delete $h->{$key};
    }

    if ( ! $h->{"auto_adapt"}) {
        if (STF_DEBUG) {
            debugf("Auto-adapt is not enabled. Skipping worker %s", $worker_name);
        }
        return;
    }

    my $threshold_key = 
        sprintf "stf.worker.%s.throttle.current_threshold", $worker_name;
    my $max_threshold_key = 
        sprintf "stf.worker.%s.throttle.threshold", $worker_name;

    my ($cur_threshold) = $config_api->search({ varname => $threshold_key });
    my ($max_threshold) = $config_api->search({ varname => $max_threshold_key });
    $cur_threshold = $cur_threshold ? $cur_threshold->{varvalue} : 0;
    $max_threshold = $max_threshold ? $max_threshold->{varvalue} : 0;

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
        if ($new_threshold < 1) {
            $new_threshold = 1;
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