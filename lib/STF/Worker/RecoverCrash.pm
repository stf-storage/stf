package STF::Worker::RecoverCrash;
use strict;
use feature 'state';
use parent qw(STF::Worker::Loop::Periodic STF::Trait::WithDBI);

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