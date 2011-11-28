use strict;
use Test::MockTime qw(restore_time set_fixed_time);
use Test::More;
use Guard qw(scope_guard);
BEGIN {
    use_ok "STF::Dispatcher";
    use_ok "STF::Constants", "SERIAL_BITS";
}

subtest 'create ID' => sub {
    scope_guard( \&restore_time );

    # ザ・ワールド！
    my $time = CORE::time();
    set_fixed_time( $time );

    # sanity (make sure time() is fixed)
    is time(), sleep 1 && time(), "sanity check";

    note "now time() is " . time();
    my $d = STF::Dispatcher->new( host_id => time() );

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

done_testing;
