package STF::API::Queue::Schwartz;
use Mouse;
use STF::Constants qw(STF_DEBUG);
use TheSchwartz;

with qw(STF::Trait::WithDBI);

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

sub get_ability {
    my ($self, $func) = @_;

    $self->ability_map->{$func};
}

sub get_client {
    my ($self) = @_;

    my $client = $self->{client};
    if (! $client) {
        my $dbh = $self->get('DB::Queue') or
            Carp::confess( "Could not fetch DB::Queue" );
        my $driver = Data::ObjectDriver::Driver::DBI->new( dbh => $dbh );
        $self->{client} = $client = TheSchwartz->new( databases => [ { driver => $driver } ] );
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

    my $client = $self->get_client();

    if ( STF_DEBUG ) {
        print STDERR "[     Queue] Engqueued $ability ($object_id)\n";
    }
    $client->insert( $ability, $object_id );
}

no Mouse;

1;

