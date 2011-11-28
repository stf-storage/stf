use strict;
use lib "lib";
use Plack::App::File;
use Plack::Builder;
use STF::Storage;

my $dir = $ENV{ STF_BACKEND_DIR };
my $app = STF::Storage->new(
    root => $dir,
    fileapp => Plack::App::File->new(root => $dir)
);

builder {
    enable "ConditionalGET";
    sub {
        my $res = $app->process(@_);
        return $res;
    }
};

