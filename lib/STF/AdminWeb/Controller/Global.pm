package STF::AdminWeb::Controller::Global;
use Mouse;
use STF::Constants qw(STORAGE_MODE_READ_WRITE STORAGE_CLUSTER_MODE_READ_WRITE);

extends 'STF::AdminWeb::Controller';

sub index {
    my ($self, $c) = @_;

    my @clusters = $c->get('API::StorageCluster')->search({});
    {
        my %stats = (
            total => 0,
            rw    => 0,
        );
        foreach my $cluster (@clusters) {
            $stats{ total }++;
            $stats{ rw    }++ if $cluster->{mode} == STORAGE_CLUSTER_MODE_READ_WRITE;
        }
        $c->stash->{clusters} = \%stats;
    }
    
    my @storages = $c->get('API::Storage')->search({});
    {
        my %stats = (
            total => 0,
            rw    => 0,
            free  => 0,
        );
        foreach my $storage (@storages) {
            $stats{ total }++;
            $stats{ rw    }++ if $storage->{mode} == STORAGE_MODE_READ_WRITE;
            $stats{ free  }++ if ! defined $storage->{cluster_id};
        }
        $c->stash->{storages} = \%stats;
    }

    my @config = $c->get('API::Config')->search({
        varname => { LIKE => "stf.global.%" }
    });
    $c->stash->{config} = \@config;
}

no Mouse;

1;
