package STF::Worker::DeleteBucket;
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
    my ($self, $bucket_id) = @_;

    local $STF::Log::PREFIX = "Worker(DB)";
    debugf("Delete bucket id = %s", $bucket_id) if STF_DEBUG;
    eval {
        $self->get('API::Bucket')->delete_objects( { id => $bucket_id } );
    };
    if ($@) {
        print "Failed to delete bucket $bucket_id: $@\n";
    }
}

no Mouse;

1;