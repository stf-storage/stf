package STF::Worker::RetireStorage;
use Mouse;

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

sub work_once {
    my $self = shift;

    eval {
        $self->get('API::Storage')->find_and_retire();
    };
    if ($@) {
        Carp::confess("Failed to retire storage: $@");
    }
}

no Mouse;

1;