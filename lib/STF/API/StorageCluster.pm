package STF::API::StorageCluster;
use Mouse;
use STF::Constants qw(STF_DEBUG STORAGE_CLUSTER_MODE_READ_WRITE);

with 'STF::API::WithDBI';

sub register_for_object {
    my ($self, $args) = @_;

    my $object_id = $args->{object_id} or die "XXX no object";
    my $cluster_id = $args->{cluster_id} or die "XXX no cluster";

    my $dbh = $self->dbh;
    $dbh->do( <<EOSQL, undef, $object_id, $cluster_id );
        INSERT INTO object_cluster_map (object_id, cluster_id) VALUES (?, ?)
EOSQL
}

sub load_for_object {
    my ($self, $object_id, $create) = @_;

    my $dbh = $self->dbh;
    my $clusters = $dbh->selectall_arrayref(<<EOSQL, { Slice => {} }, $object_id, STORAGE_CLUSTER_MODE_READ_WRITE);
        SELECT c.* 
            FROM storage_cluster c 
                JOIN object_cluster_map m ON c.id = m.cluster_id
            WHERE m.object_id = ? AND c.mode = ?
            LIMIT 1
EOSQL

    if ( @$clusters > 0 )  {
        return $clusters->[0];
    }

    if ( ! $create ) {
        return;
    }

    my $cluster = $self->calculate_for_object( $object_id );
    if (! $cluster) {
        return;
    }

    $self->register_for_object( {
        cluster_id => $cluster->{id},
        object_id  => $object_id,
    } );
    if ( STF_DEBUG ) {
        printf STDERR "[   Cluster] No cluster defined for object %s yet. Created mapping for cluster %d\n",
            $object_id,
            $cluster->{id},
        ;
    }
    return $cluster;
}

sub calculate_for_object {
    my ($self, $object_id) = @_;
    my @clusters = $self->load_writable();
    if (! @clusters) {
        return;
    }
    return $clusters[ Digest::MurmurHash::murmur_hash( $object_id ) % scalar @clusters ];
}

sub load_writable {
    my ($self, $args, $opts) = @_;

    $args ||= {};
    $args->{mode} = STORAGE_CLUSTER_MODE_READ_WRITE;

    $self->search($args, $opts);
}

no Mouse;

1;

