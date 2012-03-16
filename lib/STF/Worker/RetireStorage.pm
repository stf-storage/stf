package STF::Worker::RetireStorage;
use Mouse;

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

has '+interval' => (
    default => 5 * 60 * 1_000_000
);

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