package STF::Worker::UpdateUsage;
use strict;
use feature 'state';
use parent qw(STF::Worker::Base STF::Trait::WithContainer);

sub new {
    my $class = shift;
    $class->SUPER::new(@_, loop_class => "Periodic");
}

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
