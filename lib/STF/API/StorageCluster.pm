package STF::API::StorageCluster;
use Mouse;
use Digest::MD5 ();
use STF::Constants qw(STF_DEBUG STORAGE_CLUSTER_MODE_READ_WRITE);

with 'STF::API::WithDBI';

sub load_candidates_for {
    my ($self, $object_id) = @_;

    my @clusters = $self->load_writable();
    my %clusters = map {
        (Digest::MurmurHash::murmur_hash($_->{id} . $object_id) => $_)
    } @clusters;

    return map  { $clusters{$_} } sort { $a <=> $b } keys %clusters;
}

sub store {
    my ($self, $args) = @_;

    my $cluster   = $args->{cluster}   or die "XXX no cluster";
    my $object_id = $args->{object_id} or die "XXX no object_id";
    my $content   = $args->{content}   or die "XXX no content";;
    my $minimum   = $args->{minimum};

    my $object     = $self->get('API::Object')->lookup($object_id);
    if (! $object) {
        if (STF_DEBUG) {
            printf STDERR "[   Cluster] Could not load object to store (object_id = %s)\n",
                $object_id,
        }
        return;
    }

    my @storages = $self->get('API::Storage')->search({
        cluster_id => $cluster->{id},
    });
    if (STF_DEBUG) {
        printf STDERR "[   Cluster] Attempting to store object %s in cluster %s (want %d copies)\n",
            $object_id,
            $cluster->{id},
            defined $minimum ? $minimum : scalar @storages,
        ;
        printf STDERR "[   Cluster] Going to store in:\n";
        foreach my $storage (@storages) {
            printf STDERR "[   Cluster] + %s (id = %s)\n",
                $storage->{uri},
                $storage->{id}
            ;
        }
    }

    my $md5 = Digest::MD5->new;
    my $expected = $md5->addfile( $content )->hexdigest;
    my $entity_api = $self->get('API::Entity');
    my $stored = 0;
    # This object must be stored at least X times
    foreach my $storage (List::Util::shuffle(@storages)) {
        # if we can fetch it, don't store it
        my $fetched = $entity_api->fetch_content({
            object => $object,
            storage => $storage,
        });
        if ($fetched) {
            if ($md5->new->addfile($fetched)->hexdigest eq $expected) {
                $stored++;
                if (STF_DEBUG) {
                    printf STDERR "[   Cluster] Object %s already exist on storage %s\n",
                        $object_id,
                        $storage->{id},
                    ;
                }

                # Make sure that this entity exist in the database
                my ($entity) = $entity_api->search({
                    storage_id => $storage->{id},
                    object_id  => $object->{id}
                });
                if (! $entity) {
                    $entity_api->record({
                        storage_id => $storage->{id},
                        object_id  => $object->{id}
                    });
                }
            }
        } else {
            my $ok = $entity_api->store({
                object  => $object,
                storage => $storage,
                content => $content,
            });
            if ($ok) {
                $stored++;
            }
        }

        if ($minimum) {
            last if $stored >= $minimum;
        }
    }

    my $ok = defined $minimum ? $stored >= $minimum : scalar @storages == $stored;
    if ($ok) {
        $self->register_for_object( {
            cluster_id => $cluster->{id},
            object_id  => $object_id,
        } );
    }


    if (STF_DEBUG) {
        printf STDERR "[   Cluster] Stored %d entities in cluster %s (wanted %d)\n",
            $stored,
            $cluster->{id},
            defined $minimum ? $minimum : scalar @storages
        ;
    }
    return $ok;
}

sub check_entity_health {
    my ($self, $args) = @_;
    my $object_id = $args->{object_id} or die "XXX no object";
    my $cluster_id = $args->{cluster_id} or die "XXX no cluster";

    my @storages = $self->get('API::Storage')->search({
        cluster_id => $cluster_id
    });
    if (! @storages) {
        if (STF_DEBUG) {
            printf STDERR "[    Cluster] Could not find any storages belonging to cluster %s\n",
                $cluster_id
            ;
        }
        return ();
    }

    # only check until the first failure
    my $entity_api = $self->get('API::Entity');
    foreach my $storage (List::Util::shuffle(@storages)) {
        my $ok = $entity_api->check_health({
            object_id => $object_id,
            storage_id => $storage->{id},
        });
        if (! $ok) {
            return ();
        }
    }
    return 1;
}

sub register_for_object {
    my ($self, $args) = @_;

    my $object_id = $args->{object_id} or die "XXX no object";
    my $cluster_id = $args->{cluster_id} or die "XXX no cluster";

    if (STF_DEBUG) {
        printf STDERR "[   Cluster] Registering object %s to cluster %s\n",
            $object_id,
            $cluster_id,
        ;
    }

    my $dbh = $self->dbh;
    $dbh->do( <<EOSQL, undef, $object_id, $cluster_id );
        REPLACE INTO object_cluster_map (object_id, cluster_id) VALUES (?, ?)
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

