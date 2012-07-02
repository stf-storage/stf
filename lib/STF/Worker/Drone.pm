package STF::Worker::Drone;
use Mouse;

use File::Spec;
use File::Temp ();
use Getopt::Long ();
use List::Util ();
use Parallel::Prefork;
use Parallel::Scoreboard;
use STF::Context;

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
        Parallel::Prefork->new({
            max_workers     => $self->max_workers,
            spawn_interval  => $self->spawn_interval,
            trap_signals    => {
                map { ($_ => 'TERM') } qw(TERM INT HUP)
            }
        });
    }
);
    
has scoreboard_dir => (
    is => 'rw',
    lazy => 1,
    default => sub {
        my $sbdir = File::Temp::tempdir( CLEANUP => 1 );
        if (! -e $sbdir ) {
            if (! File::Path::make_path( $sbdir ) || ! -d $sbdir ) {
                Carp::confess("Failed to create score board dir $sbdir: $!");
            }
        }
        return $sbdir;
    }
);

has scoreboard => (
    is => 'rw',
    lazy => 1,
    default => sub {
        my $self = shift;
        return Parallel::Scoreboard->new(
            base_dir => $self->scoreboard_dir(),
        );
    }
);

has spawn_interval => (
    is => 'rw',
    default => 5
);

has workers => (
    is => 'rw',
    default => sub {
        my %workers = (
            Replicate     => 8,
            DeleteBucket  => 4,
            DeleteObject  => 4,
            ObjectHealth  => 1,
            RepairObject  => 1,
            RecoverCrash  => 1,
            RetireStorage => 1,
            StorageHealth => 0, # don't enable this by default, as it requires an extra setup step
        );
        return \%workers,
    },
    trigger => sub {
        my ($self, $new_hash) = @_;
        my %alias = (
            Usage => 'UpdateUsage',
            Retire => 'RetireStorage',
            Crash => 'RecoverCrash',
        );
        while ( my ($k, $v) = each %alias ) {
            if (exists $new_hash->{$k}) {
                $new_hash->{$v} = delete $new_hash->{$k};
            }
        }
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

sub BUILD {
    my $self = shift;
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

sub run {
    my $self = shift;

    if ( my $pid_file = $self->pid_file ) {
        open my $fh, '>', $pid_file or
            die "Could not open PID file $pid_file for writing: $!";
        print $fh $$;
        close $fh;
    }

    my $scoreboard = $self->scoreboard; # load to initialize;
    my $pp = $self->process_manager();
    while ( $pp->signal_received !~ /^(?:TERM|INT)$/ ) {
        $pp->start and next;
        eval {
            $self->start_worker( $self->get_worker() );
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

    Mouse::Util::load_class($klass)
        if ! Mouse::Util::is_class_loaded($klass);

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
    for my $worker( List::Util::shuffle(keys %$workers) ) {
        my $n = $running{$worker} || 0;
        if ( $n < $workers->{$worker} ) {
            $scoreboard->update( $worker );
            return $worker;
        }
    }

    die "Could not find a suitable worker!";
}

no Mouse;

1;
