package STF::AdminWeb::Controller;
use Mojo::Base 'Mojolicious::Controller';

sub fillinform {
    my ($self, $hash) = @_;
    $self->stash(fdat => $hash);
}

sub validate {
    my ($self, $profile, $params) = @_;
    my $result = $self->get('AdminWeb::Validator')->check( $params, $profile );
    $self->stash(result => $result);
    return $result;
}

1;
