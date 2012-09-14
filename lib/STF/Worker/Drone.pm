package
    STF::Worker::WorkerType;
use Mouse;

has name => (is => 'ro', required => 1);
has instances => (is => 'rw', default => 0);

package
    STF::Worker::Process;
use Mouse;
use STF::Constants qw(STF_DEBUG);
use STF::Log;

has worker_type => (is => 'ro', required => 1);
has pid => (is => 'rw', default => sub { $$ });

sub start {
    my ($self, $container) = @_;

    my $klass = $self->worker_type;
    $0 = sprintf '%s [%s]', $0, $klass;
    if ($klass !~ s/^\+//) {
        $klass = "STF::Worker::$klass";
    }

    Mouse::Util::load_class($klass)
        if ! Mouse::Util::is_class_loaded($klass);
    
    $klass->new(
        container => $container,
    )->work;
}

sub terminate {
    my $self = shift;
    if (STF_DEBUG) {
        debugf("Terminating worker process %d for %s", $self->pid, $self->worker_type);
    }
    kill TERM => $self->pid;
}

sub DEBLISH {
    my $self = shift;
    $self->terminate;
}


package STF::Worker::Drone;
use Mouse;

use File::Spec;
use File::Temp ();
use Getopt::Long ();
use List::Util ();
use Math::Round ();
use Parallel::ForkManager;
use STF::Context;
use STF::Constants qw(STF_DEBUG);
use STF::Log;

has context => (
    is => 'rw',
    required => 1,
);

has pid_file => (
    is => 'rw',
);

has id => (
    is => 'ro',
    default => sub {
        require Sys::Hostname;
        join '.', Sys::Hostname::hostname(), $$;
    }
);

has process_manager => (
    is => 'rw',
    required => 1,
    lazy => 1,
    builder => 'build_forkmanager',
);

has now => (
    is => 'rw'
);

has my_pid => (is => 'ro', default => sub {$$});
has is_leader => (is => 'rw', default => 0);
has next_announce => (is => 'rw', default => 0);
has last_election => (is => 'rw', default => -1);
has last_balance  => (is => 'rw', default => -1);
has last_reload   => (is => 'rw', default => -1);
has pid_to_worker_type => (is => 'ro', default => sub { +{} });
has worker_processes => (is => 'ro', default => sub { +{} });

has spawn_interval => (
    is => 'rw',
    default => 5
);

has worker_types => (
    is => 'rw',
    default => sub { [] },
);

has local_instances => (
    is => 'rw',
    default => sub { [] }
);

has gstate => (
    is => 'rw',
    default => 0
);

has spawn_timeout => (
    is => 'rw',
    default => 0
);

sub bootstrap {
    my $class = shift;

    my %opts;
    if (! Getopt::Long::GetOptions(\%opts, "config=s") ) {
        exit 1;
    }

    if ($opts{config}) {
        $ENV{ STF_CONFIG } = $opts{config};
    }
    my $context = STF::Context->bootstrap;
    $class->new(
        context => $context,
        %{ $context->get('config')->{ 'Worker::Drone' } },
    );
}

sub DEMOLISH {
    my $self = shift;
    $self->cleanup;
}

sub cleanup {
    my $self = shift;
    if ($self->my_pid != $$) {
        return;
    }

    local $STF::Log::PREFIX = "Drone";
    if (STF_DEBUG) {
        debugf("Cleanup");
    }

    foreach my $pid (keys %{$self->pid_to_worker_type}) {
        kill TERM => $pid;
    }

    $self->process_manager->wait_all_children();

    if ( my $pid_file = $self->pid_file ) {
        unlink $pid_file or
            warn "Could not unlink PID file $pid_file: $!";
    }
    local $@;
    eval {
        my $dbh = $self->get('DB::Master');
        $dbh->do(<<EOSQL, undef, $self->id);
            DELETE FROM worker_election WHERE drone_id = ?
EOSQL
    };

    my $memd = $self->get("Memcached");
    $memd->set("stf.worker.balance", Time::HiRes::time());
    if ($self->is_leader) {
        $memd->set("stf.worker.election", Time::HiRes::time());
    }
}

sub get { shift->context->container->get(@_) }

use constant {
    BIT_ELECTION => 0x001,
    BIT_BALANCE  => 0x010,
    BIT_RELOAD   => 0x100,
};
sub should_elect_leader { $_[0]->gstate & BIT_ELECTION }
sub should_balance      { $_[0]->gstate & BIT_BALANCE }
sub should_reload       { $_[0]->gstate & BIT_RELOAD }
sub check_state {
    my $self = shift;

    my $memd = $self->get('Memcached');
    my $h = $memd->get_multi(
        "stf.worker.reload",
        "stf.worker.election",
        "stf.worker.balance"
    );

    my $state = 0;
    my $when_to_reload = $h->{"stf.worker.reload"} || 0;
    if ($self->last_reload < 0 || $self->last_reload < $when_to_reload) {
        $state |= BIT_RELOAD;
    }

    my $when_to_elect = $h->{"stf.worker.election"} || 0;
    my $last_election = $self->last_election;
    if ($last_election < 0 ||                 # first time
        $self->now - $last_election > 300 ||  # it has been 5 minutes since last election
        $self->last_election < $when_to_elect # explicitly told that election should be held
    ) {
        $state |= BIT_ELECTION;
    }

    if ($self->is_leader) {
        my $when_to_balance = $h->{"stf.worker.balance"} || 0;
        my $last_balance = $self->last_balance;
        if ($last_balance < 0 || $last_balance < $when_to_balance) {
            $state |= BIT_BALANCE;
        }
    }

    if ($state != 0) {
        $self->spawn_timeout(0);
    }
    $self->gstate($state);
}

sub reload {
    my $self = shift;

    if (! $self->should_reload) {
        return;
    }

    $self->last_reload($self->now);

    # create a map so it's easier to tweak
    my $workers = $self->worker_types;
    my %map = map { ($_->name, $_) } @$workers;

    my ($name, $num_instances);
    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare(<<EOSQL);
        SELECT varname, varvalue FROM config WHERE varname LIKE 'stf.worker.%.instances'
EOSQL
    $sth->execute();
    $sth->bind_columns(\($name, $num_instances));

    while ($sth->fetchrow_arrayref) {
        $name =~ s/^stf\.worker\.([^\.]+)\.instances$/$1/;
        my $worker = delete $map{ $name };

        if (STF_DEBUG) {
            debugf("Loaded global config: %s -> %d instances", $name, $num_instances);
        }
        # If this doesn't exist in the map, then it's new. create an
        # instance
        if ($worker) {
            $worker->instances( $num_instances );
        } else {
            push @$workers, STF::Worker::WorkerType->new(
                name      => $name,
                instances => $num_instances,
            );
        }
    }

    my $instances = $dbh->selectall_arrayref(<<EOSQL, { Slice => {} }, $self->id);
        SELECT * FROM worker_instances WHERE drone_id = ?
EOSQL
    my $total = 0;
    foreach my $instance (@$instances) {
        if (STF_DEBUG) {
            debugf("Loaded local config: %s -> %d instances", $instance->{worker_type}, $instance->{instances});
        }
        $total += $instance->{instances};
    }
    $self->local_instances($instances);
    $self->process_manager->set_max_procs($total);

    if (STF_DEBUG) {
        debugf("Reloaded worker config");
    }
    return 1;
}

sub run {
    my $self = shift;

    local $STF::Log::PREFIX = "Drone";
    if ( my $pid_file = $self->pid_file ) {
        open my $fh, '>', $pid_file or
            die "Could not open PID file $pid_file for writing: $!";
        print $fh $$;
        close $fh;
    }

    my $signal_received = '';
    foreach my $signal ( qw(TERM INT HUP) ) {
        $SIG{$signal} = sub { 
            if (STF_DEBUG) {
                debugf("Drone: received signal %s", $signal);
            }
            $signal_received = $signal;
        };
    }

    while ( $signal_received !~ /^(?:TERM|INT)$/ ) {
        $signal_received = '';
        eval {
            if ($self->wait_one_child) {
                $self->spawn_timeout(0);
            }
            $self->set_now;
            $self->check_state;

            if ($self->now < $self->spawn_timeout) {
                select(undef, undef, undef, rand 5);
            } else {
                $self->announce;
                $self->elect_leader;
                $self->reload;
                if ($self->is_leader) {
                    $self->rebalance; # balance
                }
                if (! $self->spawn_children) {
                    debugf("No child created, going to wait for a while");
                    $self->spawn_timeout( $self->now + int rand 300 );
                }
            }
        };
        if (my $e = $@) {
            critf("Error during drone loop: %s", $e);
            last;
        }
    }

    $self->cleanup;
}

sub wait_one_child {
    my $self = shift;
    my $kid = $self->process_manager->wait_one_child(POSIX::WNOHANG());
    return $kid > 0 || $kid < -1;
}

sub set_now {
    my $self = shift;
    $self->now(Time::HiRes::time());
}

sub announce {
    my $self = shift;

    my $next_announce = $self->next_announce;
    if ($self->now() < $next_announce) {
        return;
    }

    # if this is our initial announce, we should tell the leader to reload
    my $dbh = $self->get('DB::Master');
    $dbh->do(<<EOSQL, undef, $self->id);
        REPLACE INTO worker_election (drone_id, expires_at) VALUES (?, UNIX_TIMESTAMP() + 300)
EOSQL
    if ($next_announce == 0) {
        $self->get("Memcached")->set("stf.worker.balance", $self->now);
    }

    $self->next_announce($self->now + 60);
    if (STF_DEBUG) {
        debugf("Announced %s", $self->id);
    }
}

sub expire_others {
    my $self = shift;
    my $dbh = $self->get('DB::Master');
    $dbh->do(<<EOSQL, undef, $self->id);
        DELETE FROM worker_election WHERE expires_at < UNIX_TIMESTAMP() AND drone_id != ?
EOSQL
}

sub elect_leader {
    my $self = shift;

    if (! $self->should_elect_leader) {
        return;
    }

    $self->last_election($self->now);
    my $dbh = $self->get('DB::Master');
    my ($drone_id) = $dbh->selectrow_array(<<EOSQL);
        SELECT drone_id FROM worker_election ORDER BY id ASC LIMIT 1
EOSQL
    # XXX Can't happen, but just in case...
    if (! defined $drone_id) {
        return;
    }

    my $is_leader = ($drone_id eq $self->id);
    if (STF_DEBUG) {
        if ($is_leader) {
            debugf("Elected myself as leader");
        }
    }
    $self->is_leader($is_leader);
    return $is_leader ? 1 :();
}

sub get_all_drones {
    my $self = shift;

    my $dbh = $self->get('DB::Master');
    my $list = $dbh->selectall_arrayref(<<EOSQL, { Slice => {} });
        SELECT * FROM worker_election
EOSQL
    return wantarray ? @$list : $list;
}

sub rebalance {
    my $self = shift;

    # Only rebalance if we need to
    if (! $self->should_balance) {
        return;
    }

    if (STF_DEBUG) {
        debugf("Rebalancing workers");
    }

    $self->last_balance( $self->now );
    # If I'm the leader, I'm allowed to expire other drones
    $self->expire_others;

    # get all the registered drones
    my @drones = $self->get_all_drones();
    my $drone_count = scalar @drones;

    # We got to clear the entire thing to do this
    my $dbh = $self->get('DB::Master');
    $dbh->do(<<EOSQL);
        DELETE FROM worker_instances;
EOSQL

    # for each registered worker types, balance it between each drone
    foreach my $worker (@{$self->worker_types}) {
        my $total_instances    = $worker->instances;
        my $instance_per_drone = 
            $total_instances <= 0 ? 0 : # safety net
            $total_instances == 1 ? 1 :
            Math::Round::round($total_instances / $drone_count);

        my $remaining = $total_instances;
        foreach my $drone (@drones) {
            last if $remaining <= 0;
            # XXX We're only "asking" to run this many processes -
            # our wish may not be fulfilled, for whatever reason.
            # Do we need to detect this?
            my $actual = $remaining < $instance_per_drone ?
                $remaining : $instance_per_drone;

            if (STF_DEBUG) {
                debugf("Balance: drone = %s, worker = %s, instances = %d", $drone->{drone_id}, $worker->name, $actual);
            }
            $dbh->do(<<EOSQL, undef, $drone->{drone_id}, $worker->name, $actual);
                REPLACE INTO worker_instances (drone_id, worker_type, instances) VALUES (?, ?, ?)
EOSQL
            $remaining -= $actual;
        }
    }

    $self->get('Memcached')->set( "stf.worker.reload", time() );
    # We came to rebalance, we should reload
    $self->gstate( $self->gstate ^ BIT_RELOAD );
    $self->reload;
}

sub get_processes_by_name {
    my ($self, $worker_type) = @_;
    my $list = $self->worker_processes->{$worker_type} ||= [];
    return wantarray ? @$list : $list;
}

sub spawn_children {
    my $self = shift;

    my $spawned = 0;
    my $instances = $self->local_instances;
    foreach my $instance (@$instances) {
        my $name  = $instance->{worker_type};
        my $count = $instance->{instances};

        my @processes = $self->get_processes_by_name($name);
        while (@processes > $count) {
            my $process = shift @processes;
            $process->terminate;
        }

        my $remaining = $count - @processes;
        while ($remaining-- > 0) {
            $spawned++;
            $self->spawn_child($name);
        }
    }
    return $spawned;
}

sub spawn_child {
    my ($self, $worker_type) = @_;

    if (STF_DEBUG) {
        debugf("Spawning child process for %s", $worker_type);
    }
    my $process = STF::Worker::Process->new(
        worker_type => $worker_type,
    );
    my $pp = $self->process_manager;
    if ($pp->start($process)) {
        return;
    }
    eval {
        $SIG{$_} = 'DEFAULT' for keys %SIG;
        $process->start($self->context->container);
    };
    if ($@) {
        critf("Failed to run child %s", $@);
    }
    $pp->finish;
}

sub build_forkmanager {
    my $self = shift;

    my $pp = Parallel::ForkManager->new;
    $pp->run_on_start(sub {
        my ($pid, $process) = @_;

        $process->pid($pid);

        my $worker_type = $process->worker_type;
        my $list = $self->worker_processes->{$worker_type} ||= [];
        push @$list, $process;
        $self->pid_to_worker_type->{$pid} = $worker_type;
    });
    $pp->run_on_finish(sub {
        my ($pid, $code, $process) = @_;

        my $worker_type = $process->worker_type;
        my $list = $self->worker_processes->{$worker_type} ||= [];
        foreach my $i (1..@$list) {
            if ($list->[$i - 1]->pid == $pid) {
                splice @$list, $i - 1, 1;
                last;
            }
        }

        delete $self->pid_to_worker_type->{$pid};
        if (STF_DEBUG) {
            debugf("Reap pid %d worker %s", $pid, $process->worker_type);
        }
    });
    return $pp;
}

no Mouse;

1;
