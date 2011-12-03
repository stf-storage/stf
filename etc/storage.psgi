use strict;
use STF::Storage;

STF::Storage->new( root => $ENV{STF_BACKEND_ROOT} )->to_app;
