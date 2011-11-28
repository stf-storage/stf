package STF::Trait::WithContainer;
use strict;
use Class::Accessor::Lite
    rw => [ qw(container) ]
;

sub get {
    my $self = shift;

    $self->container or Carp::confess("no container");
    $self->container->get(@_);
}

1;
