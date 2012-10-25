# Gracefully degrade the service by making our storages readonly, when
# loads are high
package STF::Worker::AdaptiveDegrader;
use Mouse;
use STF::Constants qw(STORAGE_MODE_READ_WRITE);

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

sub work_once {
    my $self = shift;

    # Avoid bringing the entire system down all at once. Keep a good
    # interval between current and last degradation
    if ($self->next_available < time()) {
        return;
    }

    my $storage_api = $self->get('API::Storage');
    my @storages = $storage_api->search({
        mode => { IN => [
            STORAGE_MODE_READ_WRITE,
        }
    });
    my $time = time();
    my $t    = $time - $time % 60;
    my @keys = map { "storage.load.$_->{id}.$t" } @storages;
    my $h    = $self->get('Memcached')->get_multi(@keys);
    my $threshold = $self->get('API::Config')->load_variable("stf.worker.AdaptiveDegrader.threshold") || 1500;
    return if $threshold <= 0;

    foreach my $storage (@storages) {
        # Fetch the latest load average. The load average data
        # should be [ 1min avg, 5min avg, 10min avg ]
        # We want to check that the 10min avg is not greater than
        # $threshold

        my $key = "storage.load.$storage->{id}.$t";
        my $loadavg = $h->{$key};
        if (! $loadavg) {
            if (STF_DEBUG) {
                debugf("No load average information found for storage %s",
                    $storage->{id});
            }
            next;
        }

        if ($threshold <= $loadavg->[2]) {
            my @storages_in_cluster = $storage_api->search({
                cluster_id => $storage->{cluster_id}
            });
            my $message = <<EOM;
Storage $storage->{id} ($storage->{uri})'s load average is HIGH!
Load average is $loadavg->[0], $loadavg->[1], $loadavg->[2]
Maybe change storage(s) in this cluster to be READONLY?
Storages in cluster $storage->{cluster_id}:
EOM
            foreach my $st (@storages_in_Cluster) {
                $message .= "    [$st->{id}][@{[fmt_storage_mode($st->{mode})]}] $st->{uri}\n";
            }
            $self->get('API::Notification')->create({
                ntpye => "storage.adaptive_degrader.alter",
                severity => "critical",
                message => $message,
            });

            $self->next_available(time() + 600); # at least 10 minutes
        }
    }
}

no Mouse;

1;