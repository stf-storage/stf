package STF::Worker::Drone;
use strict;
use Class::Load ();
use File::Spec;
use File::Temp qw(tempdir);
use Getopt::Long ();
use Parallel::Prefork;
use Parallel::Scoreboard;
use STF::Context;
use Class::Accessor::Lite
    rw => [ qw(
        context
        pid_file
        process_manager
        scoreboard
        scoreboard_dir
        spawn_interval
        workers
    ) ]
;

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
        interval => 5,
        %{ $context->get('config')->{ 'Worker::Drone' } },
    );
}

sub new {
    my ($class, %args) = @_;

    my $self = bless {
        spawn_interval => 1,
        workers => {
            Replicate     => 8,
            DeleteBucket  => 4,
            DeleteObject  => 4,
            ObjectHealth  => 1,
            RepairObject  => 1,
            RecoverCrash  => 1,
            RetireStorage => 1,
        },
        %args,
    }, $class;

    my %alias = (
        Usage => 'UpdateUsage',
        Retire => 'RetireStorage',
        Crash => 'RecoverCrash',
    );
    my $workers = $self->workers;
    while ( my ($k, $v) = each %alias ) {
        if (exists $workers->{$k}) {
            $workers->{$v} = delete $workers->{$k};
        }
    }

    return $self;
}

sub max_workers {
    my $self = shift;
    my $workers = $self->workers;
    my $n = 0;
    for my $v ( values %$workers ) {
        $n += $v
    }
    $n;
}

sub cleanup {
    my $self = shift;

    $self->process_manager->wait_all_children();

    if ( my $scoreboard = $self->scoreboard ) {
        $scoreboard->cleanup;
    }

    if ( my $pid_file = $self->pid_file ) {
        unlink $pid_file or
            warn "Could not unlink PID file $pid_file: $!";
    }
}

sub prepare {
    my $self = shift;

    if (! $self->scoreboard ) {
        my $sbdir = $self->scoreboard_dir  || tempdir( CLEANUP => 1 );
        if (! -e $sbdir ) {
            if (! File::Path::make_path( $sbdir ) || ! -d $sbdir ) {
                Carp::confess("Failed to create score board dir $sbdir: $!");
            }
        }

        $self->scoreboard(
            Parallel::Scoreboard->new(
                base_dir => $sbdir
            )
        );
    }

    if (! $self->process_manager) {
        my $pp = Parallel::Prefork->new({
            max_workers     => $self->max_workers,
            spawn_interval  => $self->spawn_interval,
            trap_signals    => {
                map { ($_ => 'TERM') } qw(TERM INT HUP)
            }
        });
        $self->process_manager( $pp );
    }

    if ( my $pid_file = $self->pid_file ) {
        open my $fh, '>', $pid_file or
            "Could not open PID file $pid_file for writing: $!";
        print $fh $$;
        close $fh;
    }
}

sub run {
    my $self = shift;

    $self->prepare;

    my $pp = $self->process_manager();
    while ( $pp->signal_received !~ /^(?:TERM|INT)$/ ) {
        $pp->start and next;
        eval {
            $self->start_worker( $self->get_worker );
        };
        if ($@) {
            warn "Failed to start worker ($$): $@";
        }
        print STDERR "Worker ($$) exit\n";
        $pp->finish;
    }

    $self->cleanup();
}

sub start_worker {
    my ($self, $klass) = @_;

    $0 = sprintf '%s [%s]', $0, $klass;
    if ($klass !~ s/^\+//) {
        $klass = "STF::Worker::$klass";
    }

    Class::Load::load_class($klass)
        if ! Class::Load::is_class_loaded($klass);

    print STDERR "Spawning $klass ($$)\n";

    my ($config_key) = ($klass =~ /(Worker::[\w:]+)$/);
    my $container = $self->context->container;
    my $config    = $self->context->config->{ $config_key };

    my $worker = $klass->new(
        %$config,
        cache_expires => 30,
        container => $container
    );
    $worker->work;
}

sub get_worker {
    my $self = shift;
    my $scoreboard = $self->scoreboard;

    my $stats = $scoreboard->read_all;
    my %running;
    for my $pid( keys %{$stats} ) {
        my $val = $stats->{$pid};
        $running{$val}++;
    }

    my $workers = $self->workers;
    for my $worker( keys %$workers ) {
        if ( $running{$worker} < $workers->{$worker} ) {
            $scoreboard->update( $worker );
            return $worker;
        }
    }

    die "Could not find a suitable worker!";
}

1;
