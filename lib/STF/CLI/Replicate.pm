package STF::CLI::Replicate;
use strict;
use parent qw(STF::CLI::Base);

sub run {
    my( $self, $id ) = @_;
    die "Usage: $0 Replicate <object_id>\n" unless $id;

    $self->get('API::Entity')->replicate({
        object_id => $id 
    });
}

1;

__END__
