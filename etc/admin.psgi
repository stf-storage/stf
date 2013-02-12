use strict;
use lib "lib";
use Plack::Builder;
use STF::Environment;
use STF::AdminWeb;

my $ctx = STF::Context->bootstrap;
STF::AdminWeb->new(context => $ctx)->psgi_app;
