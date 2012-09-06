package t::lib::App::Prove::Plugin::RedirectLog;
use strict;

sub load {
    if (!exists $ENV{STF_LOG_FILE}) {
        $ENV{STF_LOG_FILE} = "t/test.log";
    }
}

1;
