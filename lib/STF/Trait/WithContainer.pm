package STF::Trait::WithContainer;
use Mouse::Role;

has container => (
    is => 'ro',
    required => 1,
    handles => [ qw(get) ],
);

no Mouse::Role;

1;
