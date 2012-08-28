package t::lib::App::Prove::Plugin::StartRedis;
use strict;
use Test::More;

my $REDIS;
my $REDIS_CONF;
my $REDIS_DIR;
sub load {
    if ( ($ENV{STF_QUEUE_TYPE} || '') ne 'Resque') {
        return;
    }

    require File::Temp;
    require Test::TCP;

    $REDIS_DIR  = File::Temp::tempdir();
    $REDIS_CONF = File::Temp->new( DIR => $REDIS_DIR, SUFFIX => ".conf");
    $REDIS      = Test::TCP->new( code => sub {
        my $port = shift;

        diag "Generating temporary conf file for redis at: $REDIS_CONF";
        print $REDIS_CONF <<EOM;
daemonize no
pidfile $REDIS_DIR/redis.pid
port $port
bind 127.0.0.1
timeout 0
loglevel verbose
logfile t/redis.log
databases 16
save 900 1
save 300 10
save 60 10000
rdbcompression yes
dbfilename dump.rdb
dir $REDIS_DIR
slave-serve-stale-data yes
appendonly no
appendfsync everysec
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
slowlog-log-slower-than 10000
slowlog-max-len 128
vm-enabled no
vm-swap-file /tmp/redis.swap
vm-max-memory 0
vm-page-size 32
vm-pages 134217728
vm-max-threads 4
hash-max-zipmap-entries 512
hash-max-zipmap-value 64
list-max-ziplist-entries 512
list-max-ziplist-value 64
set-max-intset-entries 512
zset-max-ziplist-entries 128
zset-max-ziplist-value 64
activerehashing yes
EOM

        diag "Starting memcached on 127.0.0.1:$port";
        exec "redis-server $REDIS_CONF";
    } );
    $ENV{ STF_REDIS_HOSTPORT } = join ":", "127.0.0.1", $REDIS->port;
}

sub END {
    undef $REDIS;
    undef $REDIS_CONF;
    undef $REDIS_DIR;
}

1;