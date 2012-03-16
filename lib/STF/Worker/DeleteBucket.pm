package STF::Worker::DeleteBucket;
use Mouse;
use STF::Constants qw(STF_DEBUG);

extends 'STF::Worker::Base';
with 'STF::Trait::WithDBI';

has loop_class => (
    is => 'ro',
    default => sub {  $ENV{ STF_QUEUE_TYPE } || 'Q4M' }
);

sub work_once {
    my ($self, $bucket_id) = @_;

    if ( STF_DEBUG ) {
        print STDERR "Worker::DeleteBucket $bucket_id\n";
    }
    eval {
        $self->get('API::Bucket')->delete_objects( { id => $bucket_id } );
    };
    if ($@) {
        print "Failed to delete bucket $bucket_id: $@\n";
    }
}

no Mouse;

1;