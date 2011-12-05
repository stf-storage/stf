package STF::Worker::RecoverCrash;
use strict;
use feature 'state';
use parent qw(STF::Worker::Base STF::Trait::WithContainer);

sub new {
    my $class = shift;
    $class->SUPER::new(
        interval => 5 * 60,
        @_,
        loop_class => "Periodic"
    );
}

sub work_once {
    my $self = shift;

    eval {
        $self->get('API::Storage')->find_and_recover_crash();
    };
    if ($@) {
        Carp::confess("Failed to run recover crash: $@");
    }
}

1;