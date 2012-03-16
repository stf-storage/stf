package STF::AdminWeb::Controller;
use Mouse;

has view_class => (
    is => 'rw'
);

has namespace => (
    is => 'rw',
    lazy => 1,
    default => sub {
        my $self = shift;
        my $pkg = Scalar::Util::blessed($self);
        $pkg =~ s/^STF::AdminWeb::Controller:://;
        $pkg =~ s/::/\//g;
        lc $pkg;
    },
);

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

no Mouse;

1;
