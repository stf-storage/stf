use strict;
use Test::More;
use_ok "STF::Worker::Drone";

my $drone = STF::Worker::Drone->bootstrap;
ok $drone;
my $config = $drone->context->container->get('config');
local $config->{Memcached}->{namespace} = join ".", "stf.test", $$, time(), {};

subtest 'flags' => sub {
    $drone->gstate( STF::Worker::Drone::BIT_ELECTION() );
    ok $drone->should_elect_leader, "should elect leader";
    ok !$drone->should_balance, "should NOT balance";
    ok !$drone->should_reload, "should NOT reload";

    $drone->gstate( STF::Worker::Drone::BIT_BALANCE() );
    ok !$drone->should_elect_leader, "should NOT elect leader";
    ok $drone->should_balance, "should balance";
    ok !$drone->should_reload, "should NOT reload";

    $drone->gstate( STF::Worker::Drone::BIT_RELOAD() );
    ok !$drone->should_elect_leader, "should NOT elect leader";
    ok !$drone->should_balance, "should NOT balance";
    ok $drone->should_reload, "should reload";
};

subtest 'check_state' => sub {
    $drone->update_now;

    my $t = $drone->update_now + 1;
    $drone->next_check_state($t);
    $drone->check_state();

    is $drone->next_check_state, $t;

    foreach my $is_leader ( 0, 1 ) {
        my $orig = $drone->is_leader;
        my $guard = Scope::Guard->new(sub {
            note "Restoring is_leader...";
            $drone->is_leader($orig);
        });
        $drone->is_leader($is_leader);

        $drone->update_now;
        $drone->next_check_state( $drone->now - 1 );

        # Make sure that the cache doesn't contain the keys that
        # affect our states
        my $memd = $drone->context->container->get('Memcached');
        $memd->delete_multi(qw(stf.drone.reload stf.drone.election stf.drone.balance));
        $drone->check_state();

        # This is the first time, so all flags should be 1
        # except for balance, which would only be true if we're a leader
        ok $drone->should_elect_leader, "should elect leader";
        ok $drone->should_reload, "should reload";
        if ($drone->is_leader) {
            ok $drone->should_balance, "should balance";
        } else {
            ok !$drone->should_balance, "should NOT balance";
        }

        # check again. next_check_state should prevent us from doing anything
        $drone->check_state;
        ok !$drone->should_elect_leader, "check_state should not have run";
        ok !$drone->should_balance, "check_state should not have run";
        ok !$drone->should_reload, "check_state should not have run";

        # now make sure that the apparent clock is +10 minutes
        # only the election should be triggered
        # pretend we did our election, reload, and balance
        my %save = map { ($_ => $drone->$_) } qw(last_election last_reload last_balance);
        my $last_guard = Scope::Guard->new(sub {
            foreach my $key (keys %save) {
                note "Restoring $key...";
                $drone->$key($save{$key});
            }
        });
        $drone->last_election($drone->now);
        $drone->last_reload($drone->now);
        $drone->last_balance($drone->now);
        $drone->update_now;
        $drone->now($drone->now + 300 + 1);
        $drone->next_check_state(0);
        $drone->check_state;
        ok $drone->should_elect_leader, "only election should be on";
        ok ! $drone->should_balance, "!balance: only election should be on";
        ok ! $drone->should_reload, "!reload: only election should be on";

    }
};

subtest 'timers' => sub {
    $drone->update_now;
    if (! ok defined $drone->now, "now is defined") {
        die "WHAT?! bailing out";
    }

    my $save = $drone->now;
    is $drone->now, $save, "now is cached";

    sleep 1;
    $drone->update_now;
    ok $drone->now > $save;
};

    
    

done_testing;