package STF::Worker::UpdateUsage;
use strict;
use feature 'state';
use parent qw(STF::Worker::Loop::Periodic STF::Trait::WithDBI);

sub work_once {
    my $self = shift;

    eval {
        my $api = $self->get('API::Storage');
        $api->update_usage_for_all();
    };
    if ($@) {
        Carp::confess( "Failed to update usage: $@" );
    }
}

1;
