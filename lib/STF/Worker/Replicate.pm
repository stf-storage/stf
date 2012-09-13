package STF::Worker::Replicate;
use Mouse;
use STF::Constants qw(STF_DEBUG);
use STF::Log;

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
        my $object_api = $self->get('API::Object');
        if ($object_api->repair( $object_id )) {
            debugf("Replicated object %s.", $object_id) if STF_DEBUG;
        }
    };
    if ($@) {
        Carp::confess( "Failed to replicate object ID: $object_id: $@" );
    }
}

no Mouse;

1;
