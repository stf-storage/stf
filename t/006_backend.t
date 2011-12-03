use strict;
use STF::Test;
use Test::More;
use Plack::Test;
use File::Temp qw(tempdir);
use HTTP::Request::Common qw(GET PUT DELETE HEAD);
use String::Urandom ();

sub http_ok ($) {
    my $res = shift;
    my $ok  = ok( $res->is_success, "status ok" );
    if (! $ok) {
        diag explain $res;
    }
    return $ok;
}

my $dir = $ENV{ STF_STORAGE_ROOT } = tempdir(CLEANUP => 1);

test_psgi
    app => do "etc/storage.psgi",
    client => sub {
        my $cb = shift;
        my $file   = String::Urandom->new()->rand_string();

        open my $fh, '<', __FILE__;
        http_ok $cb->(PUT "/$file", Content => do { local $/; seek $fh, 0, 0; <$fh> } );

        my $res = $cb->(GET "/$file");
        if (! http_ok $res) {
        } else {
            is $res->content, do { local $/; seek $fh, 0, 0; <$fh> };
        }

        http_ok $cb->(DELETE "/$file");
        $res = $cb->(GET "/$file");
        is $res->code, 404, "GET after DELETE should fail";
    }
;

done_testing;