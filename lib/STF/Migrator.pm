package STF::Migrator;
use Mouse;
use DBIx::Connector;
use Parallel::ForkManager;
use POSIX qw(SIGTERM);
use STF::Migrator::Worker;

has connect_info => (
    is => 'ro',
    isa => 'ArrayRef',
    default => sub {
        [
            "dbi:mysql:dbname=stf",
            'FIXME',
            'FIXME',
            { RaiseError => 1, AutoCommit => 1 }
        ]
    }
);

has concurrency => (
    is => 'ro',
    default => 10,
);

has conn => (
    is => 'ro',
    isa => 'DBIx::Connector',
    lazy => 1,
    builder => "_build_conn",
);

has storage_id => (
    is => 'ro',
    required => 1,
);

has max_object_id => (
    is => 'rw',
);

has min_object_id => (
    is => 'rw',
);

has workers => (
    is => 'ro',
    default => 5
);

has segment_size => (
    is => 'ro',
    default => 5_000
);

sub BUILD {
    my $self = shift;

    if ( ! $self->max_object_id ) {
        my ($max_object_id) = $self->conn->run(sub {
            $_->selectrow_array( <<EOSQL, undef, $self->storage_id );
                SELECT object_id FROM entity WHERE storage_id = ? ORDER BY object_id DESC LIMIT 1
EOSQL
        });
        $self->max_object_id( $max_object_id );
    }

    if ( ! $self->min_object_id ) {
        my ($min_object_id) = $self->conn->run(sub {
            $_->selectrow_array( <<EOSQL, undef, $self->storage_id );
                SELECT object_id FROM entity WHERE storage_id = ? ORDER BY object_id ASC LIMIT 1
EOSQL
        });
        $self->min_object_id( $min_object_id );
    }

    return $self;
}

sub _build_conn {
    my $self = shift;
    my $connect_info = $self->connect_info;
    my $conn = DBIx::Connector->new(@$connect_info);
    $conn->mode('fixup');
    return $conn;
}

sub set_proc_name {
    my ($self, $message) = @_;

    my $fmt = "migrate-stf [%s] (%s -> %s)";
    if ( $message ) {
        $fmt .= " %s";
    }

    $0 = sprintf(
        $fmt,
        $self->storage_id,
        $self->max_object_id,
        $self->min_object_id,
        $message,
    );
}

sub run {
    my $self = shift;

    $self->set_proc_name();

    my $storage = $self->conn->run(sub {
        $_->selectrow_hashref(<<EOSQL, undef, $self->storage_id );
            SELECT * FROM storage WHERE id = ?
EOSQL
    });
    if (! $storage ) {
        die "No such storage: " . $self->storage_id;
    }

    my %children;
    my $loop = 1;
    local %SIG = %SIG;
    foreach my $sig ( qw(INT TERM) ) {
        $SIG{$sig} = sub {
            warn "Received $sig";
            $loop = 0;
            foreach my $pid (keys %children) {
                kill SIGTERM() => $pid;
            }
        };
    }

    my $pfm = Parallel::ForkManager->new($self->workers);
    my $max_object_id = $self->max_object_id;
    my $min_object_id = $self->min_object_id;
    my $segment_size  = $self->segment_size - 1;
    my $storage_id    = $self->storage_id;

    while ( $loop ) {
        my ($object_id) = $self->conn->run(sub {
            $_->selectrow_array(<<EOSQL, undef, $storage_id, $max_object_id, $min_object_id);
                SELECT e.object_id
                    FROM entity e
                    FORCE INDEX (object_id)
                    WHERE e.storage_id = ? AND e.object_id <= ? AND e.object_id > ?
                    ORDER BY e.object_id DESC LIMIT $segment_size,1
EOSQL
        });
        if (! $object_id) {
            $object_id = $min_object_id;
            $loop = 0;
        }

        my $a_worker = STF::Migrator::Worker->new(
            app => $self,
            concurrency   => $self->concurrency,
            storage_id    => $storage->{id},
            storage_uri   => $storage->{uri},
            max_object_id => $max_object_id,
            min_object_id => $object_id,
        );

        my $pid = $pfm->start;
        if ($pid) {
            $children{ $pid } = $a_worker;
            $max_object_id = $object_id;
            next;
        }

        local $SIG{TERM} = 'DEFAULT';
        local $SIG{INT}  = 'DEFAULT';
        eval {
            $a_worker->run();
        };
        if ($@) {
            print "# worker $$ failed: $@\n";
        }
        $pfm->finish;
    }

    $pfm->wait_all_children;
}

no Mouse;

1;