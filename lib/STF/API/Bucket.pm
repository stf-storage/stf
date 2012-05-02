package STF::API::Bucket;
use Mouse;
use STF::Constants qw(STF_DEBUG);

with 'STF::API::WithDBI';

sub lookup_by_name {
    my ($self, $bucket_name) = @_;
    my $dbh = $self->dbh;
    $dbh->selectrow_hashref(<<EOSQL, undef, $bucket_name);
        SELECT * FROM bucket WHERE name = ?
EOSQL
}

sub create {
    my ($self, $args) = @_;

    my $dbh = $self->dbh;
    $dbh->do(<<EOSQL, undef, $args->{id}, $args->{name});
        INSERT INTO bucket (id, name, created_at) VALUES (?, ?, UNIX_TIMESTAMP(NOW()))
EOSQL
}

sub rename {
    my ($self, $args) = @_;

    my $name = $args->{name};
    my $bucket_id = $args->{id};
    my $dbh = $self->dbh;

    my $rv = $dbh->do(<<EOSQL, undef, $name, $bucket_id );
        UPDATE bucket SET name = ? WHERE id = ?
EOSQL
    if ($rv) {
        $self->cache_delete( $self->table => $bucket_id );
    }
    return $rv;
}

sub delete {
    my ($self, $args) = @_;

    my ($id, $recursive) = @$args{ qw(id recursive) };
    my $dbh = $self->dbh;

    my $rv;
    if ($recursive) {
        # XXX We return (1) regardless
        $self->delete_objects( { id => $id } );
        $rv = 1;
        $dbh->do( <<EOSQL, undef, $id );
            DELETE FROM bucket WHERE id = ?
EOSQL
    } else {
        $rv = $dbh->do( <<EOSQL, undef, $id );
            DELETE FROM bucket
            WHERE
                bucket.id = ? AND
                ( SELECT COUNT(id) FROM object WHERE bucket_id = bucket.id LIMIT 1 ) = 0
EOSQL
    }
    if ($rv > 0) {
        $self->cache_delete( $self->table => $id ) if ! ref $id;
    }

    return $rv;
}

sub mark_for_delete {
    my ($self, $args) = @_;

    my $bucket_id = $args->{id};
    my $dbh = $self->dbh;

    my ($rv_delete, $rv_replace);
    $rv_replace = $dbh->do(
        "REPLACE INTO deleted_bucket SELECT * FROM bucket WHERE id = ?",
        undef,
        $bucket_id,
    );

    if ( $rv_replace <= 0 ) {
        if ( STF_DEBUG ) {
            printf STDERR "[  Mark Del] Failed to insert bucket %s into deleted_bucket (rv = %s)\n",
                $bucket_id,
                $rv_replace
        }
    } else {
        if ( STF_DEBUG ) {
            printf STDERR "[  Mark Del] Inserted bucket %s into deleted_bucket (rv = %s)\n",
                $bucket_id,
                $rv_replace
            ;
        }

        $rv_delete = $dbh->do(
            "DELETE FROM bucket WHERE id = ?",
            undef,
            $bucket_id,
        );

        if ( STF_DEBUG ) {
            printf STDERR "[  Mark Del] Deleted bucket %s from bucket (rv = %s)\n",
                $bucket_id,
                $rv_delete
            ;
        }
    }

    return $rv_replace && $rv_delete;
}

sub delete_objects {
    my ($self, $args) = @_;

    my $id = $args->{id};
    my $dbh = $self->dbh;

    my $object_id;
    my $queue = $self->get('API::Queue');
    my $sth = $dbh->prepare( <<EOSQL );
        SELECT id FROM object WHERE bucket_id = ?
EOSQL
    $sth->execute($id);
    $sth->bind_columns(\($object_id));

    my $object_api = $self->get('API::Object');
    while ( $sth->fetchrow_arrayref ) {
        $object_api->mark_for_delete( $object_id );
        $queue->enqueue( "delete_object", $object_id );
    }

    $dbh->do( "DELETE FROM deleted_bucket WHERE id = ?", undef, $id );
}

no Mouse;

1;
