package STF::Test;
use strict;
use parent qw(Exporter);
use lib "extlib/lib/perl5";
use Carp;
use DBI;
use File::Basename ();
use File::Path ();
use Furl;
use Guard;
use Plack::Runner;
use Proc::Guard ();
use SQL::Maker;
use Test::mysqld;
use Test::TCP;
use Test::More;
use YAML;

our @EXPORT_OK = qw(
    clear_queue
    deploy_fixtures
    split_sql_statements
    start_plackup
    start_mysqld
    start_worker
    ts_request
    write_file
);

our $MYSQLD;
our $MEMCACHED;
our @STF_BACKENDS;

{
    # $? がリークすると、prove が
    #   Dubious, test returned 15 (wstat 3840, 0xf00)
    # というので $? を localize する。
    package t::Proc::Guard;
    use parent qw(Proc::Guard);
    sub stop {
        my $self = shift;
        local $?;
        $self->SUPER::stop(@_);
    }
}

sub start_plackup {
    my ($app, @args) = @_;
    my $server = Test::TCP->new(
        code => sub {
            my $port = shift;
            my $runner = Plack::Runner->new();
            $runner->parse_options('--port' => $port, @args );
            $runner->run( $app );
        }
    );
    return $server;
}

sub start_memcached {
    my $daemonize = shift;
    note "Starting memcached...";
    my $port = Test::TCP::empty_port();
    my $memcached = t::Proc::Guard->new(
        code => sub {
            open my $logfh, '>', "t/memcached.log";
            { 
                open STDOUT, '>&', $logfh
                    or die "dup(2) failed: $!";
                open STDERR, '>&', $logfh
                    or die "dup(2) failed: $!";
                exec "memcached", ( $daemonize ? "-d" : (), "-vv", "-p", $port );
            };
            die "Failed to execute memcached: $!" if defined $!;
        },
    );
    $memcached->{port} = $port;
    note "     Started at port " . $memcached->{port};
    return $memcached;
}

sub ts_request($) {
    my $req = shift;

    $req->uri->host( $ENV{ STF_HOST } );
    $req->uri->port( $ENV{ STF_PORT } );

    my $furl = Furl->new;
    my $res  = $furl->request( $req );
    return $res->as_http_response;
}

sub start_worker {
    my $run_dir = File::Spec->catdir( qw( t run ) );
    # remove directory
    if ( -e $run_dir ) {
        if (! remove_tree( $run_dir )) {
            die "Failed to remove $run_dir: $!";
        }
    }
    # create it

    if (! make_path( $run_dir ) || ! -d $run_dir ) {
        die "Failed to create dir $run_dir: $!";
    }

    my $worker = Proc::Guard->new(
        command => [
            $^X, "bin/stf-worker", "--config", "t/config.pl",
        ]
    );
    sleep 20;

    $ENV{ _STF_WORKER } = $worker;
}

sub clear_queue {
    my $dbh = DBI->connect( $ENV{TEST_STF_QUEUE_DSN } );
    my $sth = $dbh->prepare( "SHOW TABLES" );
    $sth->execute();
    while ( my ($table) = $sth->fetchrow_array ) {
        next unless $table =~ /^queue_/;
        $dbh->do( "TRUNCATE $table" );
    }
}

1;