use strict;
use Plack::Builder;
use STF::Environment;
use STF::AdminWeb;

STF::AdminWeb->bootstrap->to_app;
