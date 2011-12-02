package STF::Worker::Replicate;
use strict;
use feature 'state';
use parent qw(STF::Worker::Base STF::Trait::WithDBI);

sub work_once {
    my ($self, $object_id) = @_;

    eval {
        $self->get('API::Entity')->replicate( {
            object_id => $object_id 
        } );
    };
    if ($@) {
        Carp::confess( "Failed to replicate object ID: $object_id: $@" );
    }
}

1;
