package STF::CLI::Base;
use strict;
use POSIX ();
use Class::Accessor::Lite (
    new => 1,
    rw => [qw(context options)]
);

sub opt_specs { (); }

sub format_time {
    my ($self, $t) = @_;
    sprintf '%s (%s)',
        POSIX::strftime('%Y-%m-%d %T', localtime($t)),
        $t
    ;
}

sub get { shift->context->get(@_) }

sub get_object {
    my ($self, $id_ish) = @_;

    if ($id_ish =~ m{^/?([^/]+)/(.+)}) { # /path/to/object ?
        my ($bucket_name, $object_name) = ($1, $2);
        my $bucket = $self->get('API::Bucket')->lookup_by_name($bucket_name);
        my $object_id = $self->get('API::Object')->find_object_id( {
            bucket_id => $bucket->{id},
            object_name => $object_name
        } );
        if ($object_id ) {
            $id_ish = $object_id;
        }
    }

    $self->get('API::Object')->lookup( $id_ish );
}

sub get_entities {
    my ($self, $object_id, $storage_id) = @_;

    my $sql = <<EOSQL;
        SELECT
            entity.*,
            CONCAT_WS('/', storage.uri, object.internal_name) as uri
        FROM
            entity JOIN object ON object.id = entity.object_id
                   JOIN storage ON storage.id = entity.storage_id
        WHERE
            object_id = ?
EOSQL
    my @binds = ( $object_id );
    if ( defined $storage_id ) {
        $sql .= <<EOSQL;
            AND storage_id = ?
EOSQL
        push @binds, $storage_id;
    }

    my $dbh = $self->get('DB::Master');
    my $entities = $dbh->prepare($sql);
    $entities->execute(@binds);
    return @{ $entities->fetchall_arrayref({}) };
}


1;
