package STF::Worker::Process;
use Mouse;
use Scalar::Util ();
use STF::Constants qw(STF_DEBUG);
use STF::Log;
use STF::Utils qw(add_resource_guard);

has context => (
    is => 'ro',
    required => 1,
);

has parent_pid => (
    is => 'ro',
    default => $$
);

has name => (
    is => 'ro',
    required => 1,
);

has election_ids => (
    is => 'ro',
    default => sub { +{} }
);

has election_count => (
    is => 'ro',
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

sub register {
    my $self = shift;

    my $ids = $self->election_ids;
    if ($self->election_count <= scalar keys %$ids) {
        return;
    }

    my $dbh = $self->get('DB::Master');
    $dbh->do(<<EOSQL, undef, $self->name, time() + 86400);
        INSERT INTO election (name, expires_at) VALUES (?, ?)
EOSQL
    my $my_id = $dbh->{mysql_insertid};
    $ids->{$my_id} = undef;
    if (STF_DEBUG) {
        debugf("Registered election token %d for %s", $my_id, $self->name);
    }

    return $my_id;
}

sub get { shift->context->container->get(@_) }

sub elect {
    my $self = shift;

    my $ids         = $self->election_ids;
    my $active_ids  = scalar grep { defined($ids->{$_}) } keys %$ids;
    my $active_want = $self->election_count;
    if ( $active_ids >= $active_want ) {
        return;
    }

    my $dbh = $self->get('DB::Master');
    # Make sure that our key actually exists
    {
        my $sth = $dbh->prepare(<<EOSQL);
            SELECT 1 FROM election WHERE id = ?
EOSQL
        foreach my $id (keys %$ids) {
            if ($sth->execute($id) < 1) {
                if (my $pid = $ids->{$id}) {
                    kill TERM => $pid;
                } else {
                    $self->unregister($id);
                }
            }
        }
    }

    my $limit = $self->election_count;
    my $sth = $dbh->prepare(<<EOSQL);
        SELECT id FROM election WHERE name = ? ORDER BY id ASC LIMIT $limit
EOSQL

    my $id;
    $sth->execute( $self->name );
    $sth->bind_columns( \($id) );
    while ($sth->fetchrow_arrayref) {
        next if ! exists $ids->{$id};
        next if $ids->{$id}; # already used

        if (STF_DEBUG) {
            debugf("Elected %s as leader of %s", $id, $self->name);
        }
        return $id;
    }

    # unregsiter $my_id, because we failed to elect ourselves
    return;
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
        container => $self->context->container,
    )->work;
}

sub unregister {
    my $self = shift;

    my $ids = $self->election_ids;
    my $id;
    if (@_>0) {
        $id = shift;
    } else {
        foreach my $x_id (keys %$ids) {
            next if ! exists $ids->{$x_id};
            next if ! $ids->{$x_id};
            $id = $x_id;
            last;
        }
    }
    return unless $id;

    if (STF_DEBUG) {
        debugf("Attempting to unregister worker %s (%s) from election",
            $self->name, $id);
    }
    eval { 
        my $dbh = $self->get('DB::Master');
        $dbh->do(<<EOSQL, undef, $id, $self->name);
            DELETE FROM election WHERE id = ? AND name = ?
EOSQL
        delete $ids->{$id};
    }; 
    if ($@) {
        critf($@);
    }
}

sub DEMOLISH { $_[0]->cleanup }

sub cleanup {
    my $self = shift;

    if ($self->parent_pid != $$) {
        return;
    }

    for my $key (keys %{$self->election_ids}) {
        $self->unregister($key) 
    }
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

my @RESOURCE_DESTRUCTION_GUARDS;
BEGIN {
    undef @RESOURCE_DESTRUCTION_GUARDS;
}

has context => (
    is => 'rw',
    required => 1,
);

has pid_file => (
    is => 'rw',
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
    default => sub {
        my $self = shift;
        my $context = $self->context;
        [
            STF::Worker::Process->new(
                name           => "Replicate",
                context        => $context,
                election_count => 8,
            ),
            STF::Worker::Process->new(
                name           => "DeleteBucket",
                context        => $context,
                election_count => 4,
            ),
            STF::Worker::Process->new(
                name           => "DeleteObject",
                context        => $context,
                election_count => 4,
            ),
            STF::Worker::Process->new(
                name           => "RepairObject",
                context        => $context,
                election_count => 4,
            ),
            STF::Worker::Process->new(
                name           => "RepairStorage",
                context        => $context,
                election_count => 1,
            ),
            STF::Worker::Process->new(
                name           => "ContinuousRepair",
                context        => $context,
                election_count => 1,
            ),
        ];
    }
);

has max_workers => (
    is => 'rw',
    default => sub {
        my $self = shift;
        my $workers = $self->workers;
        my $n = 0;
        for my $v ( @$workers ) {
            $n += $v->election_count;
        }
        $n;
    }
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

        delete $pids{$pid};
        # Tell the worker that a process belonging to the worker has
        # finished. Note that this releases an election id assocciated 
        # with the worker. This may not be necessarily matching the
        # $id that spawned the $pid, but it's okay, because
        # all we care is the number of workers spawned
        if ($data) {
            $data->[1]->unregister($data->[0]);
        }
    });
    $pp->run_on_start(sub {
        my ($pid, $data) = @_;

        $data->[1]->election_ids->{$data->[0]} = $pid;
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

    while ( $signal_received !~ /^(?:TERM|INT)$/ ) {
        $pp->wait_one_child(POSIX::WNOHANG());
        next if scalar keys %pids >= $self->max_workers;

        # First, find a worker that may be spawned. The worker must
        # win the leader-election (note that this may not be "a" leader,
        # it could be multiple leaders)
        my $to_spawn;
        my $election_id;
        foreach my $process (List::Util::shuffle(@{$self->workers})) {
            $process->register;
            next unless $election_id = $process->elect;
            $to_spawn = $process;
            last;
        }

        if (! $to_spawn) {
            sleep $self->spawn_interval;
            next;
        }

        # Now that we have a winner spawn it
        if (my $pid = $pp->start([$election_id, $to_spawn])) {
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
        $pp->finish;
    }

    foreach my $pid (keys %pids) {
        kill TERM => $pid;
    }
    $self->cleanup();
}

no Mouse;

1;
