package STF::Worker::DeleteObject;
use Mouse;
use STF::Constants qw(STF_DEBUG);

extends 'STF::Worker::Base';
with 'STF::Trait::WithDBI';

has loop_class => (
    is => 'ro',
    default => sub {  $ENV{ STF_QUEUE_TYPE } || 'Q4M' }
);

sub work_once {
    my ($self, $object_id) = @_;

    if ( STF_DEBUG ) {
        print STDERR "Worker::DeleteObject $object_id\n";
    }
    eval {
        $self->get('API::Entity')->delete_for_object_id( $object_id );
    };
    if ($@) {
        print "Failed to delete $object_id: $@\n";
    }
}

no Mouse;

1;