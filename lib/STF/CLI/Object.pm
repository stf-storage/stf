package STF::CLI::Object;
use strict;
use parent qw(STF::CLI::Base);
use JSON ();

sub optspec {
    (
        'storage=s',
        'limit=i',
    )
}

sub run {
    my ($self, $object_id) = @_;

    my $options = $self->{options};
    if ( $options->{storage} ) {
        $self->show_objects_in_storage( $options->{storage_id}, $options->{limit} );
    } else {
        my $object = $self->get_object($object_id);
        if (! $object ) {
            die "Could not find object '$object_id'";
        }

        $self->show_object( $object );
    }
}

sub show_objects_in_storage {
    my ($self, $storage_id, $limit) = @_;

    if ($limit <= 0) {
        $limit = 100;
    }

    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare(<<EOSQL);
        SELECT object.id, bucket.name as bucket_name
            FROM object JOIN bucket ON object.bucket_id = bucket.id
                        JOIN entity ON object.id = entity.object_id
            WHERE entity.storage_id = ? LIMIT ?
EOSQL
    $sth->execute( $storage_id, $limit );
    while ( my $h = $sth->fetchrow_hashref ) {
        $self->show_object( $h );
    }
}

sub show_object {
    my ($self, $object) = @_;

    my $formatter = JSON->new->pretty;
    print $formatter->encode({
        id            => $object->{id},
        path          => join( '/', $object->{bucket_name}, $object->{name} ),
        internal_name => $object->{internal_name},
        num_replicas  => $object->{num_replicas},
        size          => $object->{size},
        created_at    => $self->format_time($object->{created_at}),
        entities      => [ map {
            delete $_->{object_id};
            $_->{created_at} = $self->format_time($_->{created_at});
            $_
        } $self->get_entities( $object->{id} ) ]
    });
    print "---\n";
}

1;

