package t::lib::App::Prove::Plugin::StartBackend;
use strict;
use Test::More;
use Test::TCP;
use DBI;
use File::Path qw(make_path remove_tree);

our @STF_BACKENDS;

# Copied from AnyEvent::Util
sub close_all_fds_except {
   my %except; @except{@_} = ();

   require POSIX;

   # some OSes have a usable /dev/fd, sadly, very few
   if ($^O =~ /(freebsd|cygwin|linux)/) {
      # netbsd, openbsd, solaris have a broken /dev/fd
      my $dir;
      if (opendir $dir, "/dev/fd" or opendir $dir, "/proc/self/fd") {
         my @fds = sort { $a <=> $b } grep /^\d+$/, readdir $dir;
         # broken OS's have device nodes for 0..63 usually, solaris 0..255
         if (@fds < 20 or "@fds" ne join " ", 0..$#fds) {
            # assume the fds array is valid now
            exists $except{$_} or POSIX::close ($_)
               for @fds;
            return;
         }
      }
   }

   my $fd_max = eval { POSIX::sysconf (POSIX::_SC_OPEN_MAX ()) - 1 } || 1023;

   exists $except{$_} or POSIX::close ($_)
      for 0..$fd_max;
}

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

    require Plack::Runner;

    close_all_fds_except(1, 2);

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