package STF::API::Queue::Schwartz;
use Mouse;
use STF::Constants qw(STF_DEBUG);
use TheSchwartz;
use STF::Log;

with qw(
    STF::Trait::WithDBI
    STF::API::Queue
);

has ability_map => (
    is => 'rw',
    lazy => 1,
    builder => 'build_ability_map',
);

sub build_ability_map {
    return {
        replicate     => "STF::Worker::Replicate::Proxy",
        delete_object => "STF::Worker::DeleteObject::Proxy",
        delete_bucket => "STF::Worker::DeleteBucket::Proxy",
        repair_object => "STF::Worker::RepairObject::Proxy",
        object_health => "STF::Worker::ObjectHealth::Proxy",
    };
}

sub size_for_queue {
    my ($self, $func, $queue_name) = @_;
    my $dbh = $self->get($queue_name);
    my $ability = $self->get_ability($func);
    my ($count) = $dbh->selectrow_array( <<EOSQL, undef, $ability );
        SELECT COUNT(*) FROM
            job j JOIN funcmap f ON j.funcid = f.funcid
            WHERE f.funcname = ?
EOSQL
    return $count;
}

sub get_ability {
    my ($self, $func) = @_;

    $self->ability_map->{$func};
}

sub get_client {
    my ($self, $queue_name) = @_;

    my $client = $self->{clients}->{$queue_name};
    if (! $client) {
        my $dbh = $self->get($queue_name) or
            Carp::confess( "Could not fetch DB::Queue" );
        my $driver = Data::ObjectDriver::Driver::DBI->new( dbh => $dbh );
        $self->{client}->{$queue_name} = $client =
            TheSchwartz->new( databases => [ { driver => $driver } ] );
    }
    return $client;
}

sub enqueue {
    my ($self, $func, $object_id) = @_;

    my $ability = $self->get_ability($func);
    if (! $ability ) {
        Carp::confess( "PANIC: Don't know what the schwartz ability for $func is" );
    }

    if ( ! defined $object_id ) {
        Carp::confess("No object_id given for $func");
    }

    $self->enqueue_first_available($func, $object_id, sub {
        my ($queue_name) = @_;
        my $client = $self->get_client($queue_name);
        my $rv = $client->insert( $ability, $object_id );
        if (STF_DEBUG) {
            debugf("Enqueued %s (%s)", $ability, $object_id);
        }
        return $rv;
    });
}

no Mouse;

1;

