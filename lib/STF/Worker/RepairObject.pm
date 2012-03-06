package STF::Worker::RepairObject;
use strict;
use parent qw(STF::Worker::Base STF::Trait::WithDBI);
use STF::Constants qw(STF_DEBUG);
use Class::Accessor::Lite
    rw => [ qw(
        breadth
    ) ]
;

sub new {
    my $class = shift;
    $class->SUPER::new(
        # default for breadth: 3. i.e., if we actually repaired an object,
        # it most likely means that other near-by neighbors are broken.
        # so if we successfully repaired objects, find at most 6 neighbors
        # in both increasing/decreasing order
        breadth => 3,
        loop_class => $ENV{ STF_QUEUE_TYPE } || 'Q4M',
        @_
    );
}

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
        my $n = $object_api->repair( $object_id );
        if ( STF_DEBUG ) {
            printf STDERR "[    Repair] Repaired object %s (%d items).\n",
                $object_id,
                $n
            ;
        }
        if ( $n <= 0 ) {
            return;
        }

        if ( ! $propagate ) {
            return;
        }

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
        my @objects = $object_api->find_suspicious_neighbors( $object_id, $self->breadth );
        foreach my $neighbor ( @objects ) {
            my $key  = join '.', 'repair', 'queued', $neighbor->{id};
            my $skip = $memd->get( $key );

            if ( STF_DEBUG ) {
                printf STDERR "[    Repair] %s object health check for %s\n",
                    ($skip ? "SKIP" : "Enqueue"),
                    $neighbor->{id}
                ;
            }

            next if $skip;

            $memd->set( $key, time(), $timeout );
            $queue_api->enqueue( object_health => $neighbor->{id} );
        }
    };
    if ($@) {
        Carp::confess("Failed to repair $object_id: $@");
    }
}

1;
