package t::lib::App::Prove::Plugin::StartBackend;
use strict;
use Test::More;
use Test::TCP;
use DBI;
use File::Path qw(make_path remove_tree);

our @STF_BACKENDS;

sub load {
    diag "Checking for explicit STF_BACKEND_URLS";
    # do we have an explicit memcached somewhere?
    if ($ENV{STF_BACKEND_URLS}) {
        return;
    }

    my $max = $ENV{STF_BACKEND_COUNT} || 3;
    for my $i (1..3) {
        push @STF_BACKENDS, Test::TCP->new( code => sub {
            my $port = shift;

            my $dir = File::Spec->catfile( "t", sprintf "store%03d", $i );
            # First, cleanup previous instances
            remove_tree($dir);
            make_path($dir);

            diag "Setting up backend $i at 127.0.0.1:$port, dir = $dir";
            start_backend($i, $port, $dir);
        });
    }

    # install information in ENV, so it can be reused
    $ENV{STF_BACKEND_URLS} =
        join ",",
        map { sprintf "http://127.0.0.1:%s", $_->port } @STF_BACKENDS
    ;

    # install these backends
    my $dbh = DBI->connect( $ENV{TEST_STF_DSN}, undef,  undef, { RaiseError => 1 } );
    $dbh->do( "DELETE FROM storage" );
    my $id = 1;
    foreach my $backend (split /,/, $ENV{STF_BACKEND_URLS}) {
        $dbh->do( "INSERT INTO storage (id, uri, mode, used, capacity, created_at) VALUES ( ?, ?, 1, 0, 10000, UNIX_TIMESTAMP(NOW()))", undef, $id++, $backend );
    }
}

sub start_backend {
    my ($id, $port, $dir) = @_;

    require AnyEvent::Util;
    require Plack::Runner;

    AnyEvent::Util::close_all_fds_except(1, 2);

    open my $logfh, '>', sprintf("t/backend%03d-err.log", $id);
    open STDOUT, '>&', $logfh
        or die "dup(2) failed: $!";
    open STDERR, '>&', $logfh
        or die "dup(2) failed: $!";
    open STDIN, '<', '/dev/null' or die "closing STDIN failed: $!";
    POSIX::setsid();

    local $ENV{ STF_BACKEND_DIR } = $dir;
    my $runner = Plack::Runner->new();
    $runner->parse_options(
        "--port"       => $port,
        "--server"     => "Standalone",
        "--access-log" => sprintf("t/backend%03d-access.log", $id),
    );
    $runner->run( "t/backend.psgi" );
}

1;