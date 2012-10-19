package STF::Worker::StatsCollector;
use Mouse;
use URI;
use Net::SNMP;
use STF::Constants qw(:storage STF_DEBUG);
use STF::Log;

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

has '+interval' => (
    # XXX IF YOU CHANGE THIS, YOU NEED TO CHANGE ALL THE SNMP QUERYING!
    # See L<NORMALIZING THE KEY> below
    default => 60 * 1_000_000
);

sub work_once {
    my $self = shift;

    eval {
        local $SIG{TERM} = sub { die "Received Signal" };
        $self->collect_storage_loads();
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

sub collect_storage_loads {
    my $self = shift;

    if (STF_DEBUG) {
        debugf("Collecting storage loads.");
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

    my $time = time();
    # XXX NORMALIZING THE KEY
    # This SNMP information will be stored in $key.<time>
    # however, unless we normalize this time to some known base time
    # since we're running this worker for each 60 seconds, we use this
    # value to normalize 
    my $normalized_time = $time - $time % 60;

    my $memd = $self->get('Memcached');
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

        $memd->set(
            "storage.load.$storage->{id}.$normalized_time",
            [ map { $result->{"$baseoid.$_"} } 1..3 ],
            # XXX we want this data to survive for about 30 minutes
            1800
        );
    }
}

1;