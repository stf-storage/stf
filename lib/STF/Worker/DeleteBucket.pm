package STF::Worker::DeleteBucket;
use strict;
use feature 'state';
use parent qw(STF::Worker::Base STF::Trait::WithDBI);
use STF::Constants qw( STF_DEBUG );

sub new {
    my $class = shift;
    $class->SUPER::new(loop_class => $ENV{ STF_QUEUE_TYPE } || 'Q4M', @_);
}

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

1;