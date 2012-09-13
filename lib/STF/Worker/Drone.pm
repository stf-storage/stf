package STF::Worker::Process;
use Mouse;
use Math::Round ();
use Scalar::Util ();
use Time::HiRes ();
use STF::Constants qw(STF_DEBUG);
use STF::Log;
use STF::Utils qw(add_resource_guard);

has drone => (
    is => 'ro',
    required => 1,
    handles => {
        drone_id       => 'id',
        get            => 'get',
        update_lastmod => 'update_lastmod',
    }
);

has parent_pid => (
    is => 'ro',
    default => $$
);

has name => (
    is => 'ro',
    required => 1,
);

# the max number of instances in the entire system.
# should not be changed except for by reloading the config
has total_instances => (
    is => 'rw', 
    required => 1,
);

# the number of instances that this particular worker type
# currently thinks it should spawn. this changes depending
# on the number of workers available
has local_instances => (
    is => 'rw',
    lazy => 1,
    default => sub {
        $_[0]->total_instances;
    }
);

sub BUILD {
    my $self = shift;
    add_resource_guard(
        (sub {
            my $SELF = shift;
            Scalar::Util::weaken($SELF);
            Scope::Guard->new(sub {
                eval { $SELF->cleanup };
            });
        })->($self)
    );
}

sub renew_instances {
    my $self = shift;

    my ($id, $drone_id, $local_pid);

    my $total_instances = $self->total_instances;
    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare( <<EOSQL );
        SELECT id, drone_id, local_pid FROM worker_election 
            WHERE name = ?
            ORDER BY id ASC
            LIMIT $total_instances
EOSQL
    $sth->execute($self->name);
    $sth->bind_columns(\($id, $drone_id, $local_pid));

    my $mine = 0;
    my $local_instances = $self->local_instances;
    while ($sth->fetchrow_arrayref) {
        if ($drone_id ne $self->drone_id) {
            next;
        }

        if (++$mine > $local_instances) {
            if ( ! $local_pid) {
                my ($expires_at) = $dbh->selectrow_array(<<EOSQL, undef, $id);
                    SELECT expires_at FROM worker_election WHERE id = ?
EOSQL
                $expires_at ||= 0;
                if ($expires_at < time()) {
                    if (STF_DEBUG) {
                        debugf("Renew: Expiring token %s", $id);
                    }
                    $self->remove_token($id);
                    $self->create_token();
                }
            }
        }
    }
}

sub create_token {
    my $self = shift;

    my $total_instances = $self->total_instances;
    my $dbh = $self->get('DB::Master');

    my ($count) = $dbh->selectrow_array(<<EOSQL, undef, $self->name, $self->drone_id);
        SELECT COUNT(*) FROM worker_election WHERE name = ? AND drone_id = ?
EOSQL

    if ($count >= $total_instances) {
        return;
    }

    $dbh->do(<<EOSQL, undef, $self->name, $self->drone_id, time() + 60);
        INSERT INTO worker_election (name, drone_id, expires_at) VALUES (?, ?, ?)
EOSQL
    my $my_id = $dbh->{mysql_insertid};
    if (STF_DEBUG) {
        debugf("Registered election token %d for %s", $my_id, $self->name);
    }

    $self->update_lastmod();
}

sub active_tokens {
    my $self   = shift;
    my $list = $self->get('DB::Master')->selectall_arrayref(<<EOSQL, { Slice => {} }, $self->drone_id, $self->name);
        SELECT * FROM worker_election WHERE drone_id = ? AND name = ? AND local_pid IS NOT NULL
EOSQL
    return wantarray ? @$list : $list;
}

sub balance_load {
    my $self = shift;

    $self->create_token;

    my $total_instances = $self->total_instances;
    my $local_instances = $self->local_instances;

    my $dbh = $self->get('DB::Master');
    my ($availability) = $dbh->selectrow_array(<<EOSQL, undef, $self->name);
        SELECT count(*) FROM worker_election WHERE name = ?
EOSQL

    if ($total_instances < 1) {
        $local_instances = 0;
    } elsif ($total_instances == 1) {
        $local_instances = 1;
    } elsif ($total_instances > 1) {
        my $ratio = Math::Round::round( $availability / $total_instances );
        if ($ratio < 1) {
            $ratio = 1;
        }

        $local_instances =
            int( $total_instances / $ratio ) +
            ($total_instances % $ratio ? 1 : 0);
    }

    if (STF_DEBUG) {
        debugf("Balance: %s total = %d, limit = %d, availability = %d",
            $self->name, $total_instances, $local_instances, $availability);
    }

    my $prev = $self->local_instances();
    if ($prev == $local_instances) {
        return 0;
    }

    if (STF_DEBUG) {
        debugf("Balance: Re-calculated limit for %s (%d)",
             $self->name, $local_instances );
    }
    $self->local_instances($local_instances);

    my @active_tokens = $self->active_tokens();
    if (@active_tokens > $local_instances) {
        # clearly somebody else is also wanting to run the same worker.
        # kill excess workers
        $self->reduce_instances();
        return 1;
    }
    $self->clean_slate();
    return 0;
}

sub stop_instances {
    my $self = shift;
    my @active_tokens = $self->active_tokens;

}

sub reduce_instances {
    my $self = shift;

    my $local_instances = $self->local_instances;
    my @active_tokens = $self->active_tokens;

    my $howmany = scalar @active_tokens - $local_instances;
    if ($howmany == 0) {
        # see if we have tokens to renew
        $self->renew_instances();
        return;
    }

    # kill youngest ones until we reach the desired amount
    while ($howmany-- > 0) {
        my $token = shift @active_tokens;
        $self->remove_token($token->{id});
    }
}

sub update_expiry {
    my ($self, $token) = @_;
    my $dbh = $self->get('DB::Master');
    $dbh->do(<<EOSQL, undef, $token)
        UPDATE worker_election SET expires_at = expires_at + 300
EOSQL
}

sub elect {
    my $self = shift;

    my $total_instances = $self->total_instances;
    my $local_instances = $self->local_instances;
    my $active_tokens = $self->active_tokens;
    if (@$active_tokens >= $local_instances) {
        debugf( "%s already have %d (want %d)", $self->name, scalar @$active_tokens, $local_instances);
        $self->reduce_instances();
        return;
    }

    # We want more workers! See if we can register a new token
    $self->create_token;

    # Now do the leader election 
    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare( <<EOSQL );
        SELECT id, drone_id, local_pid FROM worker_election 
            WHERE name = ?
            ORDER BY id ASC
            LIMIT $total_instances
EOSQL

    my $mine = 0;
    my ($token, $drone_id, $pid);
    $sth->execute($self->name);
    $sth->bind_columns(\($token, $drone_id, $pid));
    while ($sth->fetchrow_arrayref) {
        if ($drone_id ne $self->drone_id) {
            # XXX not me. go next
            next;
        }

        last if ($mine++ >= $local_instances);
        if ($pid) {
            if (! kill 0 => $pid) {
                # WTF, the process does not exist?
                $self->remove_token($token);
            }
            next;
        }

        # This is us!
        $dbh->do(<<EOSQL, undef, $token);
            UPDATE worker_election SET local_pid = -1 WHERE id = ?
EOSQL

        if (STF_DEBUG) {
            debugf("Elected token %s for %s", $token, $self->name);
        }
        return $token;
    }

    $self->clean_slate();
    return;
}

sub clean_slate {
    my $self = shift;
    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare(<<EOSQL);
        SELECT id, drone_id, local_pid FROM worker_election WHERE expires_at < ?
EOSQL

    $sth->execute(time());
    my ($token, $drone_id, $local_pid);
    $sth->bind_columns(\($token, $drone_id, $local_pid));
    while ($sth->fetchrow_arrayref) {
        if ($drone_id ne $self->drone_id) {
            # just delete old stuff that's not ours
            $dbh->do("DELETE FROM worker_election WHERE expires_at = ?", undef, $token);
            next;
        }

        if (kill 0 => $local_pid) {
            $self->update_expires($token);
        } else {
            $self->remove_token($token);
        }
    }
}
        
sub start {
    my $self = shift;

    my $klass = $self->name;
    $0 = sprintf '%s [%s]', $0, $klass;
    if ($klass !~ s/^\+//) {
        $klass = "STF::Worker::$klass";
    }

    Mouse::Util::load_class($klass)
        if ! Mouse::Util::is_class_loaded($klass);

    $klass->new(
        container => $self->drone->context->container,
    )->work;
}

sub associate_pid {
    my ($self, $token, $pid) = @_;
    my $dbh = $self->get('DB::Master');
    $dbh->do(<<EOSQL, undef, $pid, $token);
        UPDATE worker_election SET local_pid = ? WHERE id = ?
EOSQL
    debugf("Associated pid %d for token %s", $pid, $token);
}

sub reap {
    my ($self, $token) = @_;
    $self->get('DB::Master')->do(<<EOSQL, undef, $token);
        DELETE FROM worker_election WHERE id = ?
EOSQL
}

sub remove_token {
    my ($self, $token) = @_;

    return unless $token;

    if (STF_DEBUG) {
        debugf("Attempting to remove token for worker %s (%s) from election",
            $self->name, $token);
    }

    my $dbh = $self->get('DB::Master');
    my ($pid) = $dbh->selectrow_array(<<EOSQL, undef, $token);
        SELECT local_pid FROM worker_election WHERE id = ?
EOSQL
    if ($pid) {
        kill TERM => $pid;
    } else {
        $dbh->do(<<EOSQL, undef, $token);
            DELETE FROM worker_election WHERE id = ?
EOSQL
    }
}

sub DEMOLISH { $_[0]->cleanup }

sub cleanup {
    my $self = shift;

    if (STF_DEBUG) {
        debugf("cleanup (parent %d, me %d)", $self->parent_pid, $$);
    }

    if ($self->parent_pid != $$) {
        return;
    }

    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare( <<EOSQL );
        SELECT id FROM worker_election WHERE drone_id = ? AND name = ?
EOSQL

    my $token;
    $sth->execute( $self->drone_id, $self->name );
    $sth->bind_columns( \($token) );
    while ($sth->fetchrow_arrayref) {
        $self->remove_token($token);
    }
    $self->update_lastmod;
}


package STF::Worker::Drone;
use Mouse;

use File::Spec;
use File::Temp ();
use Getopt::Long ();
use List::Util ();
use Parallel::ForkManager;
use Parallel::Scoreboard;
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
    builder => sub {
        my $self = shift;
        Parallel::ForkManager->new($self->max_workers);
    }
);
    
has spawn_interval => (
    is => 'rw',
    default => 5
);

has workers => (
    is => 'rw',
    default => sub { [] },
);

has max_workers => (
    is => 'rw',
    default => sub {
        my $self = shift;
        my $workers = $self->workers;
        my $n = 0;
        for my $v ( @$workers ) {
            $n += $v->total_instances;
        }
        $n;
    }
);

has last_modified => (
    is => 'rw',
    default => 0,
);

has last_reload => (
    is => 'rw',
    default => 0,
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

sub cleanup {
    my $self = shift;

    $self->process_manager->wait_all_children();

    if ( my $pid_file = $self->pid_file ) {
        unlink $pid_file or
            warn "Could not unlink PID file $pid_file: $!";
    }

    foreach my $worker (@{$self->workers}) {
        $worker->cleanup;
    }
}

sub get { shift->context->container->get(@_) }

sub update_lastmod {
    my $self = shift;
    my $time = Time::HiRes::time();
    $self->get('Memcached')->set("stf.worker.lastmod", $time);
    $self->last_modified($time);
}

sub check_lastmod {
    my $self = shift;
    my $time = $self->get('Memcached')->get("stf.worker.lastmod");
    $time ||= -1;
    if ($time > $self->last_modified) {
        $self->last_modified($time);
        return 1;
    }
    return;
}

sub reload {
    my $self = shift;

    my $last_reload = $self->last_reload;
    my $when_to_reload = $self->get('Memcached')->get("stf.config.reload");
    $when_to_reload ||= time();
    if ($last_reload >= $when_to_reload) {
        # no need to relead
        return;
    }

    # create a map so it's easier to tweak
    my $workers = $self->workers;
    my %map = map { ($_->name, $_) } @$workers;

    my ($name, $instances) = @_;
    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare(<<EOSQL);
        SELECT varname, varvalue FROM config WHERE varname LIKE 'stf.worker.%.instances'
EOSQL
    $sth->execute();
    $sth->bind_columns(\($name, $instances));

    my $max_workers = 0;
    while ($sth->fetchrow_arrayref) {
        $name =~ s/^stf\.worker\.([^\.]+)\.instances$/$1/;
        my $worker = delete $map{ $name };

        # If this doesn't exist in the map, then it's new. create an
        # instance
        if ($worker) {
            $worker->total_instances( $instances );
        } else {
            push @$workers, STF::Worker::Process->new(
                name            => $name,
                drone           => $self,
                total_instances => $instances,
            );
        }
        $max_workers += $instances;
    }

    $self->max_workers($max_workers);
    $self->process_manager()->set_max_procs($max_workers);
    $self->last_reload($when_to_reload);
    $self->last_modified(0);
    if (STF_DEBUG) {
        debugf("Reloaded worker config");
    }
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

    my $pp = $self->process_manager();

    my $signal_received = '';
    my %pids;
    $pp->run_on_finish(sub {
        my ($pid, $status, $data) = @_;

        if (STF_DEBUG) {
            debugf( "Reaped worker process %d", $pid);
        }

        if (! delete $pids{$pid}) {
            local $Log::Minimal::AUTODUMP = 1;
            critf("What the...? didn't find pid %d in list of children", $pid);
            critf("%s", \%pids);
        }
        # Tell the worker that a process belonging to the worker has
        # finished. Note that this releases an election id assocciated 
        # with the worker. This may not be necessarily matching the
        # $id that spawned the $pid, but it's okay, because
        # all we care is the number of workers spawned
        if ($data) {
            $data->[1]->reap($data->[0]);
        }
    });
    $pp->run_on_start(sub {
        my ($pid, $data) = @_;
        my ($id, $worker) = @$data;
        $worker->associate_pid($id, $pid);
        $signal_received = '';
    });

    foreach my $signal ( qw(TERM INT HUP) ) {
        $SIG{$signal} = sub { 
            if (STF_DEBUG) {
                debugf("Received signal %s", $signal);
            }
            $signal_received = $signal;
        };
    }

    my $spawn_timeout = 0;
    my $balance_timeout = 0;
    while ( $signal_received !~ /^(?:TERM|INT)$/ ) {
        if ($pp->wait_one_child(POSIX::WNOHANG()) > 0) {
            $spawn_timeout = 0;
        }

        # check if we should reload our config
        $self->reload();

        # if the somebody has updated the lastmod counter, then we probably
        # need to re-calculate our numbers
        if ($self->check_lastmod) {
            if (STF_DEBUG) {
                debugf("Last modified has been updated, clearing spawn_timeout");
            }
            $spawn_timeout = 0;

            my $workers = $self->workers;
            # Query how many workers are available. This will determine
            # the number of max workers and average worker instances per
            # worker group
            my $killed = 0;
            foreach my $worker (@$workers) {
                $killed += $worker->balance_load;
            }
            if ($killed) {
                # give the drone a chance to collect the children
                # before spawning new instances
                next;
            }
        }

        my $now = time();
        if ($spawn_timeout > $now) {
            my $remaining = $spawn_timeout - $now;
            select(undef, undef, undef, rand($remaining > 5 ? 5 : $remaining));
            next;
        }

        # win the leader-election (note that this may not be "a" leader,
        # it could be multiple leaders)
        my $to_spawn;
        my $token;
        my $workers = $self->workers;
        foreach my $process (List::Util::shuffle(@$workers)) {
            next unless $token = $process->elect;
            $to_spawn = $process;
            last;
        }

        if (! $to_spawn) {
            # There was nothing to spawn ... to save us from having to
            # send useless queries to the database, sleep for an extended
            # amount of time
            $spawn_timeout = $now + int rand 30;
            next;
        }

        # Now that we have a winner spawn it
        if (my $pid = $pp->start([$token, $to_spawn])) {
            $pids{$pid}++;
            sleep $self->spawn_interval;
            next;
        }

        # Child process
        foreach my $signal ( keys %SIG ) {
            $SIG{$signal} = 'DEFAULT';
        }
        eval { $to_spawn->start };
        if ($@) { critf($@) }
        if (STF_DEBUG) {
            debugf("Child exiting for %s (%d)", $to_spawn->name, $$);
        }
        $pp->finish;
    }

    foreach my $pid (keys %pids) {
        if (STF_DEBUG) {
            debugf("Sending TERM to %d", $$);
        }
        kill TERM => $pid;
    }
    if (STF_DEBUG) {
        debugf("Terminating drone... calling cleanup()");
    }
    $self->cleanup();
}

no Mouse;

1;
