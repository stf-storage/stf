package STF::CLI::Crash;
use strict;
use parent qw(STF::CLI::Base);

sub run {
    my( $self, $id ) = @_;
    die "Usage: $0 crash <storage_id>\n" unless $id;

    my $storage_api = $self->get('API::Storage');
    if ( ! $storage_api->lookup( $id ) ) {
        printf STDERR "No such storage '%d'\n", $id;
        exit 0;
    }
    $storage_api->recover_crash( $id );
}

1;
