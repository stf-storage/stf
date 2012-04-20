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
    };
    if ($@) {
        Carp::confess("Failed to repair $object_id: $@");
    }
}

no Mouse;

1;
