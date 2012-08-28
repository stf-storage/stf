package STF::Test;
BEGIN {
    $ENV{DEPLOY_ENV} = 'test';
}
use strict;
use parent qw(Exporter);
use Carp;
use DBI;
use Plack::Runner;
use Proc::Guard ();
use Test::TCP;
use Test::More;
use Log::Minimal ();
$Log::Minimal::LOG_LEVEL ||= "NONE";


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
    no warnings 'redefine';
    my $queue_type = $ENV{STF_QUEUE_TYPE} || 'Q4M';
    if ($queue_type eq 'Resque') {
        *clear_queue = \&clear_queue_resque;
    } elsif ($queue_type eq 'Redis') {
        *clear_queue = \&clear_queue_redis;
    } else {
        *clear_queue = \&clear_queue_dbi;
    }
    goto &clear_queue;
}

sub clear_queue_dbi {
    my $dbh = DBI->connect( $ENV{STF_QUEUE_DSN } );
    my $sth = $dbh->prepare( "SHOW TABLES" );
    $sth->execute();
    while ( my ($table) = $sth->fetchrow_array ) {
        next unless $table =~ /^queue_|^job$/;
        $dbh->do( "TRUNCATE $table" );
    }
}

sub clear_queue_redis {
    require Redis;
    my $redis = Redis->new(server => $ENV{STF_REDIS_HOSTPORT});
    foreach my $qname (qw(replicate repair_object delete_object delete_bucket)) {
        $redis->del($qname);
    }
}

sub clear_queue_resque {
    require Resque;
    my $resque = Resque->new(redis => $ENV{STF_REDIS_HOSTPORT});
    foreach my $qname ($resque->queues) {
        $resque->remove_queue($qname);
    }
}

# String::URandomとか使っても良いけど面倒くさい
sub random_string {
    my @chars = ('a'..'z');
    join "", map { $chars[ rand @chars ] } 1..($_[0] || 8);
}

1;