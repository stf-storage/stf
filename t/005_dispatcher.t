use strict;
use Test::MockTime qw(restore_time set_fixed_time);
use Test::More;
use Guard qw(scope_guard);
BEGIN {
    use_ok "STF::Dispatcher";
    use_ok "STF::Constants", "SERIAL_BITS";
    use_ok "Plack::Util";
}

subtest 'create ID' => sub {
    scope_guard( \&restore_time );

    # ザ・ワールド！
    my $time = CORE::time();
    set_fixed_time( $time );

    # sanity (make sure time() is fixed)
    is time(), sleep 1 && time(), "sanity check";

    note "now time() is " . time();

    local $ENV{STF_HOST_ID} = time();
    my $d = STF::Dispatcher->bootstrap(config => "t/config.pl");

    my @ids = map { $d->create_id } 1..10;

    # we got 10 "good" results
    is scalar( grep { defined $_ && /^\d+$/ } @ids ), scalar( @ids ), "got 'good' results";

    # ...and they are all unique
    my %h = map { ($_ => 1) } @ids;
    is scalar( keys %h ), scalar( @ids ), "values are unique";

    # muck with shared memory value
    my $shm = $d->shared_mem;
    my $max = ( 1 << SERIAL_BITS ) - 1;
    $shm->write( pack("ql", time(), $max), 0, 24 );

    eval { $d->create_id };
    like $@, qr/serial bits overflowed/, "got overflow";

    # 時は再び動き出す・・・そしてまたザ・ワールド！
    set_fixed_time( $time + 5 );
    note "now time() is " . time();
    eval { $d->create_id };
    ok ! $@, "no overflow";
};

subtest 'enqueue timeout' => sub {
    my $ctxt = STF::Context->bootstrap(config => "t/config.pl");
    my $d    = STF::Dispatcher->new(
        cache_expires => 300,
        container => $ctxt->container,
        context   => $ctxt,
        host_id   => time()
    );

    my $dbh = $ctxt->container->get('DB::Queue');
    # XXX hack to get the call to stall
    local $dbh->{Callbacks} = {
        do => sub {
            sleep 10;
        },
    };

    my $buf = '';
    open my $stderr, '>', \$buf;
    eval {
        local *STDERR = $stderr;
        alarm(10);
        local $SIG{ALRM} = sub { die "BAD TIMEOUT" };
        $d->enqueue( replicate => 1 );
    };
    alarm(0);
    ok !$@, "enqueue timed out 'silently'";
    like $buf, qr/timeout_call timed out/;
};

done_testing;
