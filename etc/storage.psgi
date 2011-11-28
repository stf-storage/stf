use strict;
use STF::Storage;

my $app = STF::Storage->new( root => $ENV{STF_BACKEND_ROOT} );
sub { $app->process(@_) }
