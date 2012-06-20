use strict;
use Test::More;
use Plack::Request;
use Plack::Runner;
use Plack::Test;
use HTTP::Request::Common qw(PUT DELETE);
use STF::Test qw(clear_queue);
use STF::Constants qw(:storage);

use_ok "STF::Context";
use_ok "STF::Worker::StorageHealth";

sub make_psgi {
    my %args = @_;

    my $put_ok    = $args{put};
    my $get_ok    = $args{get};
    my $head_ok   = $args{head};
    my $delete_ok = $args{delete};

    my $content;
    return Test::TCP->new( code => sub {
        my $port = shift;
        my $runner = Plack::Runner->new;
        $runner->parse_options("-p", $port);
        $runner->run(sub {
            my $req = Plack::Request->new(shift);
            my $method = $req->method;
            if ($method eq 'PUT' && $put_ok) {
                my $fh = $req->body;
                $content = do { local $/; <$fh> };
                return [ 204, [], [] ];
            } elsif ($method eq 'GET' && $get_ok) {
                return [ 200, [], [$content] ];
            } elsif ($method eq 'HEAD' && $head_ok) {
                return [ 200, [], [] ];
            } elsif ($method eq 'DELETE' && $delete_ok) {
                undef $content;
                return [ 201, [], [] ];
            }
            return [500, [], []];
        });
    } );
}

subtest 'run worker' => sub {
    my $context = STF::Context->bootstrap( config => "t/config.pl" );
    my $container = $context->container;

    my $guard = $container->new_scope();
    my $dbh = $container->get( 'DB::Master' );
    my $storage_api = $container->get('API::Storage');

    my @storages = $storage_api->search();
    my %storages = map { ($_->{id} => 1) } @storages;
    my $key = 1;
    while ( exists $storages{$key} ) {
        $key++;
    }

    # fail cases
    my @cases = (
        {},
        { put => 1 },
        { put => 1, head => 1 },
        { put => 1, head => 1, get => 1 },
    );

    foreach my $case (@cases) {
        $storage_api->delete($key);

        my $server = make_psgi(%$case);

        # register
        $storage_api->create({
            id  => $key,
            uri => sprintf("http://127.0.0.1:%d", $server->port),
            mode => STORAGE_MODE_READ_WRITE,
            created_at => time(),
        });

        # check state
        my $storage = $storage_api->lookup($key);
        if (ok $storage, "created storage $key") {
            is $storage->{mode}, STORAGE_MODE_READ_WRITE, "storage is writable";
        }
        # now start the StorageHealth worker
        {
            my $worker = STF::Worker::StorageHealth->new(
                container => $context->container,
                max_works_per_child => 1,
            );
            $worker->work;
        }

        # check state
        $storage = $storage_api->lookup($key);
        if (ok $storage, "got storage for $key") {
            is $storage->{mode}, STORAGE_MODE_TEMPORARILY_DOWN, "storage is now down";
        }
    }

    { # this should not bring the storage down
        $storage_api->delete($key);
        my $server = make_psgi(get => 1, head => 1, put => 1, delete => 1);

        # register
        $storage_api->create({
            id  => $key,
            uri => sprintf("http://127.0.0.1:%d", $server->port),
            mode => STORAGE_MODE_READ_WRITE,
            created_at => time(),
        });

        # check state
        my $storage = $storage_api->lookup($key);
        if (ok $storage, "created storage $key") {
            is $storage->{mode}, STORAGE_MODE_READ_WRITE, "storage is writable";
        }
        # now start the StorageHealth worker
        {
            my $worker = STF::Worker::StorageHealth->new(
                container => $context->container,
                max_works_per_child => 1,
            );
            $worker->work;
        }

        # check state
        $storage = $storage_api->lookup($key);
        if (ok $storage, "got storage for $key") {
            is $storage->{mode}, STORAGE_MODE_READ_WRITE, "storage is NOT down";
        }
    }
};

done_testing;