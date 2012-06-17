package t::lib::App::Prove::Plugin::RedirectLog;
use strict;

sub load {
    $ENV{STF_LOG_FILE} ||= "t/test.log";
}

1;
