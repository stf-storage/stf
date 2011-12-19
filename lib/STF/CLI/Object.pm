package STF::CLI::Object;
use strict;
use parent qw(STF::CLI::Base);
use JSON ();

sub opt_specs {
    (
        'all!',
        'storage=s',
        'limit=i',
    )
}

sub run {
    my ($self, $object_id) = @_;

    my $options = $self->{options};
    if ( $options->{storage} ) {
        $self->show_objects_in_storage( $options->{storage}, $options->{limit} );
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
        SELECT object.id
            FROM object JOIN bucket ON object.bucket_id = bucket.id
                        JOIN entity ON object.id = entity.object_id
            WHERE entity.storage_id = ? LIMIT ?
EOSQL
    $sth->execute( $storage_id, $limit );
    while ( my $h = $sth->fetchrow_hashref ) {
        my $object = $self->get_object( $h->{id} );
        $self->show_object( $object );
    }
}

sub show_object {
    my ($self, $object) = @_;

    my $formatter = JSON->new->pretty;

    my $h = {
        id            => $object->{id},
        path          => join( '/', $object->{bucket_name}, $object->{name} ),
        internal_name => $object->{internal_name},
        num_replica   => $object->{num_replica},
        size          => $object->{size},
        created_at    => $self->format_time($object->{created_at}),
    };
    if ($self->{options}->{all}) {
        $h->{ entities } = [ map {
            delete $_->{object_id};
            $_->{created_at} = $self->format_time($_->{created_at});
            $_
        } $self->get_entities( $object->{id} ) ];
    }
    print $formatter->encode( $h );
    print "---\n";
}

1;

