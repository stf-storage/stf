package t::lib::App::Prove::Plugin::StartBackend;
use strict;
use Test::More;
use Test::TCP;
use DBI;
use File::Path qw(make_path remove_tree);

our @STF_STORAGES;

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
    diag "Checking for explicit STF_STORAGE_URLS";
    # do we have an explicit memcached somewhere?
    if ($ENV{STF_STORAGE_URLS}) {
        return;
    }

    my $max = $ENV{STF_STORAGE_COUNT} ||= 6;
    for my $i (1..$max) {
        push @STF_STORAGES, Test::TCP->new( code => sub {
            my $port = shift;

            my $name = sprintf "storage%03d", $i;
            $0 = "stf storage server '$name' on port $port";
            my $dir = File::Spec->catfile( "t", $name );
            # First, cleanup previous instances
            remove_tree($dir);
            make_path($dir);

            diag "Setting up storage $i at 127.0.0.1:$port, dir = $dir";
            start_storage($i, $port, $dir);
        });
    }

    # install information in ENV, so it can be reused
    $ENV{STF_STORAGE_URLS} =
        join ",",
        map { sprintf "http://127.0.0.1:%s", $_->port } @STF_STORAGES
    ;

    # install these storages
    my $dbh = DBI->connect( $ENV{STF_MYSQL_DSN}, undef,  undef, { RaiseError => 1 } );
    $dbh->do( "DELETE FROM storage" );
    $dbh->do( "DELETE FROM storage_cluster" );

    my $num_clusters = $max % 3 == 0 ? $max / 3 : int($max/3) + 1;
    for my $i ( 1.. $num_clusters ) {
        diag "Registering cluster $i";
        $dbh->do( "INSERT INTO storage_cluster (id, mode) VALUES (?, 1)", undef, $i );
    }
    $ENV{STF_STORAGE_CLUSTERS} = $num_clusters;

    my $id = 1;
    my $cluster_id = 1;
    foreach my $storage (split /,/, $ENV{STF_STORAGE_URLS}) {
        diag "Registering storage $id for cluster $cluster_id";
        $dbh->do( "INSERT INTO storage (id, cluster_id, uri, mode, created_at) VALUES ( ?, ?, ?, 1, UNIX_TIMESTAMP(NOW()))", undef, $id, $cluster_id, $storage );
        if ( $id % 3 == 0 ) {
            $cluster_id++;
        }
        $id++;
    }
    
}

sub start_storage {
    my ($id, $port, $dir) = @_;

    require Plack::Runner;

    close_all_fds_except(1, 2);

    open my $logfh, '>', sprintf("t/storage%03d-err.log", $id);
    open STDOUT, '>&', $logfh
        or die "dup(2) failed: $!";
    open STDERR, '>&', $logfh
        or die "dup(2) failed: $!";
    open STDIN, '<', '/dev/null' or die "closing STDIN failed: $!";
    POSIX::setsid();

    unshift @INC, "lib";
    local $ENV{ STF_STORAGE_ROOT } = $dir;
    my $runner = Plack::Runner->new();
    $runner->parse_options(
        "--port"       => $port,
        "--server"     => "Standalone",
        "--access-log" => sprintf("t/storage%03d-access.log", $id),
    );
    $runner->run( "etc/storage.psgi" );
}

1;