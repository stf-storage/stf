package STF::Worker::RepairObject;
use Mouse;
use STF::Constants qw(STF_DEBUG);

extends 'STF::Worker::Base';
with 'STF::Trait::WithDBI';

# default for breadth: 3. i.e., if we actually repaired an object,
# it most likely means that other near-by neighbors are broken.
# so if we successfully repaired objects, find at most 6 neighbors
# in both increasing/decreasing order
has breadth => (
    is => 'rw',
    default => 3
);

has '+loop_class' => (
    default => sub {
        $ENV{ STF_QUEUE_TYPE } || 'Q4M',
    }
);

sub work_once {
    my ($self, $object_id) = @_;

    my $propagate = 1;
    if ($object_id =~ s/^NP://) {
        $propagate = 0;
    }
    eval {
        my $object_api = $self->get('API::Object');
        my $queue_api = $self->get('API::Queue');

        # returns the number of repaired entities.
        my ($n, $broken) = $object_api->repair( $object_id );
        if ( STF_DEBUG ) {
            printf STDERR "[    Repair] Repaired object %s (%d items).\n",
                $object_id,
                defined $n ? $n : 0,
            ;
        }
        if ( !defined $n || $n <= 0 ) {
            return;
        }

        if ( ! $propagate ) {
            return;
        }

        if (! $broken || ! @$broken) {
            return;
        }

        # XXX Not sending stuff to object health for now
        return;

        if ( STF_DEBUG ) {
            printf STDERR "[    Repair] Going to enqueue neighbors to object health queue (%s)\n",
                $object_id
            ;
        }
        # If we indeed got a broken object, then other objects
        # in the near vicinity are likely to be missing. Find
        # objects with close IDs and check these

        my $memd = $self->get( 'Memcached' );
        my $timeout = time() + 3600;

        # Only look for objects that are in the REPAIRED storage.
        # This is in $broken. It is possible that we have $n > 0 and
        # @$broken == 0, because we might have just had an object
        # that had too few replicas
        my @objects = $object_api->find_suspicious_neighbors( {
            object_id => $object_id,
            storages  => [ map { $_->{id} } @$broken ],
            breadth   => $self->breadth
        });

        my %keys = map {
            (join('.', 'repair', 'queued', $_->{id}), $_)
        } @objects;

        my $h = $memd->get_multi( keys %keys );
        my @to_get = grep { ! $h->{$_} } keys %keys;
        foreach my $memd_key ( @to_get ) {
            my $neighbor = $keys{ $memd_key };
            my $key  = join '.', 'repair', 'queued', $neighbor->{id};
            if ( STF_DEBUG ) {
                printf STDERR "[    Repair] Enqueue object health check for %s\n",
                    $neighbor->{id}
                ;
            }
            $queue_api->enqueue( object_health => $neighbor->{id} );
        }
        $memd->set_multi( map { [ $_ => 1, 3 * 60 * 60 ] } @to_get );
    };
    if ($@) {
        Carp::confess("Failed to repair $object_id: $@");
    }
}

no Mouse;

1;
