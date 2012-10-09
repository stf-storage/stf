package STF::Worker::DeleteObject;
use Mouse;
use STF::Constants qw(STF_DEBUG);
use STF::Log;

extends 'STF::Worker::Base';
with 'STF::Trait::WithDBI';

has loop_class => (
    is => 'ro',
    default => sub {  $ENV{ STF_QUEUE_TYPE } || 'Q4M' }
);

sub work_once {
    my ($self, $object_id) = @_;

    local $STF::Log::PREFIX = "Worker(DO)";
    debugf("Delete object id = %s", $object_id) if STF_DEBUG;
    eval {
        $self->get('API::Entity')->delete_for_object_id( $object_id );
    };
    if ($@) {
        print "Failed to delete $object_id: $@\n";
    }
}

no Mouse;

1;