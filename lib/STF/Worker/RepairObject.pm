package STF::Worker::RepairObject;
use strict;
use parent qw(STF::Worker::Base STF::Trait::WithDBI);
use STF::Constants qw(STF_DEBUG);
use Class::Accessor::Lite
    rw => [ qw(
        breadth
    ) ]
;

sub work_once {
    my ($self, $object_id) = @_;

    if ( STF_DEBUG ) {
        print STDERR "Worker::RepairObject $object_id\n";
    }
    eval {
        my $object_api = $self->get('API::Object');
        my $queue_api = $self->get('API::Queue');

        # returns the number of repaired entities.
        my $n = $object_api->repair( $object_id );
        if ( $n > 0 ) {
            # If we indeed got a broken object, then other objects
            # in the near vicinity are likely to be missing. Find
            # objects with close IDs and check these
            if ( STF_DEBUG ) {
                printf STDERR "[    Repair] Repaired %d items. Going to check neighbors\n",
                    $n
                ;
            }

            my $memd = $self->get( 'Memcached' );
            my $timeout = 3600;
            foreach my $neighbor ( $object_api->find_neighbors( $object_id, $self->breadth ) ) {
                my $key = join '.', 'repair', 'queued', $neighbor->{id};
                next if $memd->get( $key );

                if ( STF_DEBUG ) {
                    printf STDERR "[    Repair] Enqueue repair for %s\n",
                        $neighbor->{id}
                    ;
                }

                $memd->set( $key, time(), $timeout );
                $queue_api->enqueue( repair_object => $neighbor->{id} );
            }
        }
    };
    if ($@) {
        Carp::confess("Failed to replicate $object_id: $@");
    }
}

1;
