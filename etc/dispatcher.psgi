use strict;
use lib "lib";
use Plack::Builder;
use STF::Environment;
use STF::Dispatcher;
use STF::Dispatcher::PSGI;

my $dispatcher = STF::Dispatcher->bootstrap;
my $app = STF::Dispatcher::PSGI->new( impl => $dispatcher )->to_app;
if ( $ENV{ USE_PLACK_REPROXY } ) {
    if( STF::Constants::STF_DEBUG() ) {
        print "[Dispatcher] Enabling Plack::Middleware::Reproxy::Furl\n";
    }
    require Plack::Middleware::Reproxy::Furl;
    $app = Plack::Middleware::Reproxy::Furl->wrap( $app );
}
return $app;


