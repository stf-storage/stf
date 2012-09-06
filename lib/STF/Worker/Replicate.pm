package STF::Worker::Replicate;
use Mouse;

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

has '+loop_class' => (
    default => sub {
        $ENV{ STF_QUEUE_TYPE } || 'Q4M',
    }
);

sub work_once {
    my ($self, $object_id) = @_;

    eval {
        $self->get('API::Object')->repair( $object_id );
    };
    if ($@) {
        Carp::confess( "Failed to replicate object ID: $object_id: $@" );
    }
}

no Mouse;

1;
