package STF::CLI::Status;
use strict;
use parent qw(STF::CLI::Base);
use JSON; # XXX FIXME

sub opt_specs { ( 'update' ); }

sub run {
    my( $self, $id ) = @_;

    my $object = $self->get('API::Object')->status_for( $id );
    print JSON->new->utf8->pretty->encode( $object );
}


1;

__END__

