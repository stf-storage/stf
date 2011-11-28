package STF::CLI::Object;
use strict;
use parent qw(STF::CLI::Base);
use POSIX ();
use JSON ();

sub opt_specs {
    (
        'path=s',
        'storage=s',
        'limit=i',
    );
}

sub run {
    my $self = shift;

    if ( my $path = $self->options->{path} ) {
        $self->show_object_from_path( $path );
    } elsif ( my $storage = $self->options->{storage} ) {
        $self->show_objects_in_storage( $storage, $self->options->{limit} || 10 );
    } else {
        die "Must specify path or storage";
    }
}

sub show_object_from_path {
    my ($self, $path) = @_;

    if ($path !~ m{^/?([^/]+)/(.+)}) {
        die "Could not parse $path";
    }

    my ($bucket_name, $object_name) = ($1, $2);

    my $bucket = $self->get('API::Bucket')->lookup_by_name($bucket_name);
    my $object_id = $self->get('API::Object')->find_object_id( {
        bucket_id => $bucket->{id},
        object_name => $object_name
    } );
    if (! $object_id ) {
        die "Could not find object '$path'";
    }
    $self->show_object( $self->get('API::Object')->lookup( $object_id ) );
}

sub show_objects_in_storage {
    my ($self, $storage_id, $limit) = @_;

    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare(<<EOSQL);
        SELECT object.*, bucket.name as bucket_name
            FROM object JOIN bucket ON object.bucket_id = bucket.id
                        JOIN entity ON object.id = entity.object_id
            WHERE entity.storage_id = ? AND object.id > ? LIMIT ?
EOSQL
    $sth->execute( $storage_id, 0, $limit );
    while ( my $h = $sth->fetchrow_hashref ) {
        $self->show_object( $h );
    }
}

sub show_object {
    my ($self, $object) = @_;

    my $dbh = $self->get('DB::Master');
    my $entities = $dbh->prepare(<<EOSQL);
        SELECT entity.*, CONCAT_WS('/', storage.uri, object.internal_name) as uri
            FROM entity JOIN object ON object.id = entity.object_id
                        JOIN storage ON storage.id = entity.storage_id
            WHERE object_id = ?
EOSQL

    my $fmt_time = sub {
        my $t = shift;
        sprintf '%s (%s)',
            POSIX::strftime('%Y-%m-%d %T', localtime($t)),
            $t
        ;
    };

    my $formatter = JSON->new->pretty;
    $entities->execute( $object->{id} );

    print $formatter->encode({
        id => $object->{id},
        path => join( '/', $object->{bucket_name}, $object->{name} ),
        internal_name => $object->{internal_name},
        num_replicas => $object->{num_replicas},
        size => $object->{size},
        created_at => $fmt_time->($object->{created_at}),
        entities => [ map {
            delete $_->{object_id};
            $_->{created_at} = $fmt_time->($_->{created_at});
            $_
        } @{ $entities->fetchall_arrayref({}) } ]
    });
    print "---\n";
}

1;

