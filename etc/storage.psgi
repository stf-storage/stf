use strict;
use STF::Storage;

STF::Storage->new( root => $ENV{STF_STORAGE_ROOT} )->to_app;
