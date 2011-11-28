package STF::Worker::RetireStorage;
use strict;
use parent qw(STF::Worker::Loop::Periodic STF::Trait::WithDBI);

sub work_once {
    my $self = shift;

    eval {
        $self->get('API::Storage')->find_and_retire();
    };
    if ($@) {
        Carp::confess("Failed to retire storage: $@");
    }
}

1;