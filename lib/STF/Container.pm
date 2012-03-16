package STF::Container;
use Mouse;
use Guard ();

has objects => (
    is => 'ro',
    default => sub { +{} }
);

has registry => (
    is => 'ro',
    default => sub { +{} },
);

has scoped_objects => (
    is => 'rw',
    default => sub { +{} },
);

has scoped_registry => (
    is => 'rw',
    default => sub { +{} },
);

sub new_scope {
    my ($self, $initialize) = @_;
    $self->scoped_objects({}) if $initialize;
    return Guard::guard( sub { $self->scoped_objects({}) } );
}

sub get {
    my ($self, $key) = @_;

    my $object;
    my $is_scoped = exists $self->scoped_registry->{$key};
    if ( $is_scoped ) {
        $object = $self->scoped_objects->{ $key };
    } else {
        # if it's a regular object, just try to grab it
        $object = $self->objects->{$key};
    }

    if (! $object) {
        my $code;
        if ( $is_scoped ) {
            my $code = $self->scoped_registry->{$key};
            if ($object = $code->($self)) {
                $self->scoped_objects->{$key} = $object;
            }
        } elsif ( $code = $self->registry->{$key} ) {
            if ( $object = $code->($self) ) {
                $self->objects->{$key} = $object;
            }
        }
    }

    if ( ! $object) {
        Carp::confess("$key could not be found in container");
    }

    return $object;
}

sub register {
    my ($self, $key, $thing, $opts) = @_;

    $opts ||= {};
    if (ref $thing eq 'CODE') {
        if ($opts->{scoped}) {
            $self->scoped_registry->{$key} = $thing;
        } else {
            $self->registry->{$key} = $thing;
        }
    } else {
        $self->objects->{$key} = $thing;
    }
}

no Mouse;

1;
