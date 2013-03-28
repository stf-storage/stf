use strict;
use lib "lib";
use Plack::Builder;
use STF::Environment;
use STF::Dispatcher;
use STF::Dispatcher::PSGI;

use constant HAS_ACCESS_LOG => !!$ENV{STF_DISPATCHER_ACCESS_LOG};

my $rotatelogs;
if ($ENV{STF_DISPATCHER_ACCESS_LOG}) {
    require File::RotateLogs;
    my $linkname = $ENV{STF_DISPATCHER_ACCESS_LOG};
    $rotatelogs = File::RotateLogs->new(
        logfile => "$linkname.%Y%m%d%H",
        linkname        => $linkname,
        rotationtime    => $ENV{STF_DISPATCHER_LOG_ROTATTION_TIME} || 86400,
        maxage          => $ENV{STF_DISPATCHER_LOG_MAXAGE} || 14 * 86400,
    );
}

my $dispatcher = STF::Dispatcher->bootstrap;
my $app = STF::Dispatcher::PSGI->new( impl => $dispatcher )->to_app;
if ( $ENV{ USE_PLACK_REPROXY } ) {
    if( STF::Constants::STF_DEBUG() ) {
        print "[Dispatcher] Enabling Plack::Middleware::Reproxy::Furl\n";
    }
    require Plack::Middleware::Reproxy::Furl;
    $app = Plack::Middleware::Reproxy::Furl->wrap( $app );
}

builder {
    if (HAS_ACCESS_LOG) {
        enable 'AxsLog' => (
            response_time => 1,
            logger => sub { $rotatelogs->print(@_) }
        );
    }
    $app;
};


