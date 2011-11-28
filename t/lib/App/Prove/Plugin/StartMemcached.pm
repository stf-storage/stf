package t::lib::App::Prove::Plugin::StartMemcached;
use strict;
use Test::More;
use Test::TCP;

our @MEMCACHED;

sub load {
    diag "Checking for explicit TEST_MEMCACHED_SERVERS";
    # do we have an explicit memcached somewhere?
    if (my $servers = $ENV{TEST_MEMCACHED_SERVERS}) {
        return;
    }

    my $max = $ENV{TEST_MEMCACHED_COUNT} || 3;
    for my $i (1..3) {
        push @MEMCACHED, Test::TCP->new(code => sub {
            my $port = shift;
            diag "Starting memcached $i on 127.0.0.1:$port";
            exec "memcached -l 127.0.0.1 -p $port";
        });
    }

    $ENV{TEST_MEMCACHED_SERVERS} = join ",",
        map { '127.0.0.1:' . $_->port } @MEMCACHED;
}

1;