# Watches if objects have the desired number of entities.
# If an object is found where the number of entities does not satisfy the
# minimum requirement, then it sends a job to the repair workers
#
# Must be triggered by *something*, but it usually comes from RepairObject
# worker, which in turn is triggered by API::Object's get method

package STF::Worker::ObjectHealth;
use strict;
use parent qw(STF::Worker::Base STF::Trait::WithDBI);
use STF::Constants qw(STF_DEBUG);

sub new {
    my $class = shift;
    $class->SUPER::new(
        loop_class => $ENV{ STF_QUEUE_TYPE } || 'Q4M',
        @_
    );
}

sub work_once {
    my ($self, $object_id) = @_;

    # WTF?
    if (! defined $object_id || length $object_id <= 0) {
        print STDERR "[    Health] empty object_id\n";
        return;
    }

    my $object_api  = $self->get('API::Object');
    my ($valids, $invalids) = $object_api->check_health( $object_id );
    if (! @$invalids) {
        if ( STF_DEBUG ) {
            print STDERR "Worker::ObjectHealth $object_id does not need repair\n";
        }
    }

    my $queue_api = $self->get('API::Queue');
    $queue_api->enqueue( repair_object => $object_id );
}

1;
