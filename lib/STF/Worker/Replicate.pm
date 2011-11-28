package STF::Worker::Replicate;
use strict;
use feature 'state';
use parent qw(STF::Worker::Loop::Q4M STF::Trait::WithDBI);
use STF::Constants ();

sub work_once {
    my ($self, $object_id) = @_;

    my $guard = $self->container->new_scope();
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
