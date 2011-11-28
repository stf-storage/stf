package STF::AdminWeb::Controller;
use strict;
use Class::Accessor::Lite
    new => 1,
    rw => [ qw(
        view_class
    ) ]
;

sub namespace {
    my $self = shift;
    $self->{namespace} ||= do {
        my $pkg = Scalar::Util::blessed($self);
        $pkg =~ s/^STF::AdminWeb::Controller:://;
        $pkg =~ s/::/\//g;
        lc $pkg;
    };
}

sub execute {
    my ($self, $c, $action) = @_;
    $self->$action($c);
}

sub fillinform {
    my ($self, $c, $hash) = @_;
    $c->stash->{fdat} = $hash;
}

sub validate {
    my ($self, $c, $profile, $params) = @_;
    my $result = $c->get('AdminWeb::Validator')->check( $params, $profile );
    $c->stash->{result} = $result;
    return $result;
}

1;
