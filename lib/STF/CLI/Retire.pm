package STF::CLI::Retire;
use strict;
use parent qw(STF::CLI::Base);

sub run {
    my( $self, $id ) = @_;
    die "Usage: $0 Retire <storage_id>\n" unless $id;
    $self->get('API::Storage')->retire( $id );
}

1;

__END__
