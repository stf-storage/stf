package STF::AdminWeb::Controller;
use Mojo::Base 'Mojolicious::Controller';

sub fillinform {
    my ($self, $hash) = @_;
    $self->stash(fdat => $hash);
}

1;
