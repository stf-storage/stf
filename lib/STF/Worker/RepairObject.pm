package STF::Worker::RepairObject;
use Mouse;
use STF::Constants qw(STF_DEBUG);
use STF::Log;

extends 'STF::Worker::Base';
with 'STF::Trait::WithDBI';

has '+loop_class' => (
    default => sub {
        $ENV{ STF_QUEUE_TYPE } || 'Q4M',
    }
);

sub work_once {
    my ($self, $object_id) = @_;

    local $STF::Log::PREFIX = "Repair(W)";

    # legacy
    my $propagate = 1;
    if ($object_id =~ s/^NP://) {
        $propagate = 0;
    }

    # XXX We're cramming this in the same worker because it's more costly
    # to create a q4m queue for this
    if ( $object_id =~ /:/ ) {
        my $storage_id;
        ($object_id, $storage_id) = split /:/, $object_id;
        eval {
            my $entity_api = $self->get('API::Entity');
            $entity_api->repair( $object_id, $storage_id );
        };
        if ($@) {
            Carp::confess("Failed to repair entity for object $object_id on storage $storage_id: $@");
        }
    } else {
        eval {
            my $object_api = $self->get('API::Object');
            if ($object_api->repair( $object_id )) {
                debugf("Repaired object %s.", $object_id) if STF_DEBUG;
            }
        };
        if ($@) {
            Carp::confess("Failed to repair $object_id: $@");
        }
    }
}

no Mouse;

1;
