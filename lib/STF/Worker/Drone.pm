package
    STF::Worker::WorkerType;
use Mouse;

has name => (is => 'ro', required => 1);
has instances => (is => 'rw', default => 0);

package STF::Worker::Drone;
use Mouse;
use Config ();
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

has cleanup_completed => (is => 'rw', default => 0);
has my_pid => (is => 'ro', default => sub {$$});
has is_leader => (is => 'rw', default => 0);
has next_announce => (is => 'rw', default => 0);
has next_check_state => (is => 'rw', default => 0);
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
    if (! $self->cleanup_completed) {
        $self->cleanup;
    }
}

sub cleanup {
    my $self = shift;
    if ($self->my_pid != $$) {
        return;
    }

    local $STF::Log::PREFIX = "Drone";
    if (STF_DEBUG) {
        debugf("Commencing cleanup for drone");
    }

    local $SIG{PIPE} = 'IGNORE';

    my $pm = $self->process_manager;
    local %SIG = %SIG;
    if (!$pm) {
        # What what what what?! 
        $SIG{CHLD} = sub { 
            while (1) {
                my $pid = waitpid(-1, POSIX::WNOHANG());
                if ($pid == 0 || $pid == -1) {
                    last;
                }
                debugf("Reaped %d (after process manager has been released)", $pid);
            }
        };
    }
    my $worker_processes = $self->worker_processes;
    foreach my $worker_type (keys %$worker_processes) {
        foreach my $pid (@{$worker_processes->{$worker_type}}) {
            $self->terminate_child($worker_type, $pid);
        }
    }

    if ($pm) {
        $pm->wait_all_children();
    }

    if ( my $pid_file = $self->pid_file ) {
        if (STF_DEBUG) {
            debugf("Releasing pid file %s", $pid_file);
        }

        if (-f $pid_file) {
            unlink $pid_file or
                warn "Could not unlink PID file $pid_file: $!";
        }
    }

    {
        local $@;
        eval {
            if (STF_DEBUG) {
                debugf("Deleting id %s from worker_election", $self->id);
            }
            my $dbh = $self->get('DB::Master');
            $dbh->do(<<EOSQL, undef, $self->id);
                DELETE FROM worker_election WHERE drone_id = ?
EOSQL
        };
        if ($@) {
            critf("Error whle deleting my ID %s from worker_election", $self->id);
        }
    }

    eval {
        $self->broadcast_reload;
    };

    wait;

    $self->cleanup_completed(1);
    if (STF_DEBUG) {
        debugf("Cleanup complete");
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

    if ($self->next_check_state > $self->now) {
        return;
    }

    $self->next_check_state($self->now + rand(10));
    my $memd = $self->get('Memcached');
    my $h = $memd->get_multi(
        "stf.drone.reload",
        "stf.drone.election",
        "stf.drone.balance"
    );

    my $state = 0;
    my $when_to_reload = $h->{"stf.drone.reload"} || 0;
    my $last_reload = $self->last_reload;
    if ($last_reload < 0 ||                # first time
        $self->now - $last_reload > 600 || # it has been 10 minutes since last reload
        $last_reload < $when_to_reload     # explicitly told that election should be held
    ) {
        $state |= BIT_RELOAD;
    }

    my $when_to_elect = $h->{"stf.drone.election"} || 0;
    my $last_election = $self->last_election;
    if ($last_election < 0 ||                 # first time
        $self->now - $last_election > 300 ||  # it has been 5 minutes since last election
        $self->last_election < $when_to_elect # explicitly told that election should be held
    ) {
        $state |= BIT_ELECTION;
    }

    if ($self->is_leader) {
        my $when_to_balance = $h->{"stf.drone.balance"} || 0;
        my $last_balance = $self->last_balance;
        if ($last_balance < 0 ||
            $self->now - $last_balance > 600 || # it has been 10 minutes since last balance
             $last_balance < $when_to_balance
        ) {
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

    if (STF_DEBUG) {
        debugf("Reloading drone configuration");
    }
    $self->last_reload($self->now);

    # create a map so it's easier to tweak
    my $workers = $self->worker_types;
    my %map = map { ($_->name, $_) } @$workers;

    my ($name, $num_instances);
    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare(<<EOSQL);
        SELECT varname, varvalue FROM config WHERE varname LIKE 'stf.drone.%.instances'
EOSQL
    $sth->execute();
    $sth->bind_columns(\($name, $num_instances));

    while ($sth->fetchrow_arrayref) {
        $name =~ s/^stf\.drone\.([^\.]+)\.instances$/$1/;
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

    # What?! No instances?
    if (! @$instances) {
        # Am I the leader? Make sure we rebalance our workers ASAP
        if ($self->is_leader) {
            $self->gstate( $self->gstate ^ BIT_BALANCE );
        } else {
            # We're not the leader. tell the leader to get something up
            $self->broadcast_reload();
            if (STF_DEBUG) {
                debugf("No local instances provided for us. Sent notice to reload");
            }
        }
        return;
    }

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
        debugf("Workers that %s needs to spawn:", $self->id);
        foreach my $instance (@$instances) {
            debugf(" + %s = %d instances", $instance->{worker_type}, $instance->{instances} );
        }
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
            die "Received signal during loop\n";
        };
    }

    $self->join_group;

    while ( $signal_received !~ /^(?:TERM|INT)$/ ) {
        $signal_received = '';
        eval {
            if ($self->wait_one_child) {
                $self->spawn_timeout(0);
            }
            $self->update_now;
            $self->check_state;
            $self->announce;

            if ($self->now < $self->spawn_timeout) {
                select(undef, undef, undef, rand 5);
            } else {
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
            if ($e =~ /^Received signal during loop$/) {
                next;
            } else {
                critf("Error during drone loop: %s", $e);
                last;
            }
        }
    }

    $self->cleanup;
}

sub wait_one_child {
    my $self = shift;
    my $kid = $self->process_manager->wait_one_child(POSIX::WNOHANG());
    return $kid > 0 || $kid < -1;
}

sub update_now {
    $_[0]->now(Time::HiRes::time());
}

sub broadcast_reload {
    my $self = shift;
    if (STF_DEBUG) {
        debugf("Broadcast reload");
    }
    my $time = $self->now + 5;
    $self->get('Memcached')->set_multi(
        [ "stf.drone.reload"   => $time ],
        [ "stf.drone.election" => $time ],
        [ "stf.drone.balance"  => $time ],
    );
}

sub join_group {
    my $self = shift;
    $self->update_now;
    $self->announce;
    $self->broadcast_reload;
}

sub announce {
    my $self = shift;

    my $next_announce = $self->next_announce;
    if ($self->now() < $next_announce) {
        return;
    }

    # if this is our initial announce, we should tell the leader to reload
    my $dbh = $self->get('DB::Master');
    my ($id) = $dbh->selectrow_array(<<EOSQL, undef, $self->id);
        SELECT id FROM worker_election WHERE drone_id = ?
EOSQL
    if (defined $id) {
        $dbh->do(<<EOSQL, undef, $id);
            UPDATE worker_election SET expires_at = UNIX_TIMESTAMP() + 300 WHERE id = ?
EOSQL
    } else {
        $dbh->do(<<EOSQL, undef, $self->id);
            INSERT worker_election (drone_id, expires_at) VALUES (?, UNIX_TIMESTAMP() + 300)
EOSQL
    }
    if ($next_announce == 0) {
        $self->get("Memcached")->set("stf.drone.balance", $self->now);
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

    if (STF_DEBUG) {
        debugf("Running election for leader...");
    }

    $self->last_election($self->now);
    my $dbh = $self->get('DB::Master');
    my ($drone_id) = $dbh->selectrow_array(<<EOSQL);
        SELECT drone_id FROM worker_election ORDER BY id ASC LIMIT 1
EOSQL

    if (STF_DEBUG) {
        debugf("Election: elected %s, my id is %s", $drone_id || '(null)', $self->id);
    }

    # XXX Can't happen, but just in case...
    if (! defined $drone_id) {
        $self->is_leader(0);
        return;
    }

    my $is_leader = ($drone_id eq $self->id);
    if (STF_DEBUG) {
        if ($is_leader) {
            debugf("Elected myself as leader");
        }
    }
    $self->is_leader($is_leader);

    # XXX for backwards compatiblity. old configuration variables
    # need to be converted.
    {
        my $sth = $dbh->prepare(<<EOSQL);
            SELECT varname FROM config WHERE varname LIKE 'stf.worker.%.instances'
EOSQL
        $sth->execute();

        my $varname;
        $sth->bind_columns(\($varname));
        while ($sth->fetchrow_arrayref) {
            my $new_varname = $varname;
            $new_varname =~ s/^stf\.worker\./stf.drone./;

            $dbh->do(<<EOSQL, undef, $new_varname, $varname);
                UPDATE config SET varname = ? WHERE varname = ?
EOSQL
        }
    }

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

    # XXX Currently we assume that the entire set of workers
    # that we care about are going to be available in the 
    # config database
    my $dbh = $self->get('DB::Master');

    # for each registered worker types, balance it between each drone
    foreach my $worker (@{$self->worker_types}) {
        my $total_instances    = $worker->instances;
        my $instance_per_drone = 
            $total_instances <= 0 ? 0 : # safety net
            $total_instances == 1 ? 1 :
            Math::Round::round($total_instances / $drone_count);

        if (STF_DEBUG) {
            debugf("Total instances for worker %s is %d (%d per drone)",
                $worker->name,
                $total_instances,
                $instance_per_drone
            );
        }

        my $remaining = $total_instances;
        my $last = $drones[-1];
        foreach my $drone (@drones) {
            last if $remaining <= 0;
            # XXX We're only "asking" to run this many processes -
            # our wish may not be fulfilled, for whatever reason.
            # Do we need to detect this?
            my $actual =
                $remaining < $instance_per_drone ? $remaining :
                $last == $drone ? $remaining :
                $instance_per_drone
            ;

            if (STF_DEBUG) {
                debugf("Balance: drone = %s, worker = %s, instances = %d", $drone->{drone_id}, $worker->name, $actual);
            }

            my ($current) = $dbh->selectrow_array(<<EOSQL, undef, $drone->{drone_id}, $worker->name);
                SELECT instances FROM worker_instances WHERE drone_id = ? AND worker_type = ?
EOSQL
            if (defined $current) {
                if (STF_DEBUG) {
                    debugf("Balance: UPDATE %s, %s, %d (was %d)", $drone->{drone_id}, $worker->name, $actual, $current);
                }
                $dbh->do(<<EOSQL, undef, $actual, $drone->{drone_id}, $worker->name);
                    UPDATE worker_instances SET instances = ? WHERE drone_id = ? AND worker_type = ?
EOSQL
            } else {
                if (STF_DEBUG) {
                    debugf("Balance: UPDATE %s, %s, %d", $drone->{drone_id}, $worker->name, $actual);
                }
                $dbh->do(<<EOSQL, undef, $drone->{drone_id}, $worker->name, $actual);
                    INSERT INTO worker_instances (drone_id, worker_type, instances) VALUES (?, ?, ?)
EOSQL
            }
            $remaining -= $actual;
        }
    }

    # We came to rebalance, we should reload
    $self->gstate( $self->gstate ^ BIT_RELOAD );
    $self->reload;
    $self->broadcast_reload();
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
            my $pid = shift @processes;
            $self->terminate_child($name, $pid);
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
    my $pp = $self->process_manager;
    if ($pp->start($worker_type)) {
        return;
    }
    eval {
        local $ENV{PERL5LIB} = join ":", @INC;
        exec $Config::Config{perlpath}, '-e', <<EOM;
use strict;
use STF::Context;
use STF::Worker::$worker_type;

\$0 = "$0 [$worker_type]";

my \$cxt = STF::Context->bootstrap;
my \$container = \$cxt->container;
my \$config = \$cxt->config->{'Worker::${worker_type}'} || {};
my \$worker = STF::Worker::${worker_type}->new(
    %\$config,
    container => \$container,
);
\$worker->work;
EOM
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
        my ($pid, $worker_type) = @_;

        my $list = $self->worker_processes->{$worker_type} ||= [];
        push @$list, $pid;
        $self->pid_to_worker_type->{$pid} = $worker_type;
    });
    $pp->run_on_finish(sub {
        my ($pid, $code, $worker_type) = @_;

        my $list = $self->worker_processes->{$worker_type} ||= [];
        foreach my $i (1..@$list) {
            if ($list->[$i - 1] == $pid) {
                splice @$list, $i - 1, 1;
                last;
            }
        }
        delete $self->pid_to_worker_type->{$pid};

        if (STF_DEBUG) {
            debugf("Reap pid %d worker %s", $pid, $worker_type);
        }
    });
    return $pp;
}

sub terminate_child {
    my ($self, $worker_type, $pid) = @_;
    if (STF_DEBUG) {
        debugf("Terminating worker process %d for %s", $pid, $worker_type);
    }
    kill TERM => $pid;
}


no Mouse;

1;
