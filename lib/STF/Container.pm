package STF::Container;
use strict;
use Guard;
use Class::Accessor::Lite
    rw => [ qw(
        objects
        registry
        scoped_objects
        scoped_registry
    ) ]
;

sub new {
    my ($class, %args) = @_;
    bless {
        objects => {},
        registry => {},
        scoped_registry => {},
        scoped_objects  => {},
        %args
    }, $class;
}

sub new_scope {
    my ($self, $initialize) = @_;
    $self->scoped_objects({}) if $initialize;
    return guard { $self->scoped_objects({}) };
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

1;
