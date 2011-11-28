package STF::Worker::DeleteObject;
use strict;
use feature 'state';
use parent qw(STF::Worker::Loop::Q4M STF::Trait::WithDBI);
use STF::Constants qw(STF_DEBUG);

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

1;