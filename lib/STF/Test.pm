package STF::Test;
use strict;
use parent qw(Exporter);
use Carp;
use DBI;
use Plack::Runner;
use Proc::Guard ();
use Test::TCP;
use Test::More;

our @EXPORT_OK = qw(
    clear_queue
    deploy_fixtures
    start_plackup
    write_file
    random_string
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

sub clear_queue {
    my $dbh = DBI->connect( $ENV{TEST_STF_QUEUE_DSN } );
    my $sth = $dbh->prepare( "SHOW TABLES" );
    $sth->execute();
    while ( my ($table) = $sth->fetchrow_array ) {
        next unless $table =~ /^queue_|^job$/;
        $dbh->do( "TRUNCATE $table" );
    }
}

# String::URandomとか使っても良いけど面倒くさい
sub random_string {
    my @chars = ('a'..'z');
    join "", map { $chars[ rand @chars ] } 1..($_[0] || 8);
}

1;