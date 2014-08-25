package STF::API::StorageCluster;
use Mouse;
use Digest::MD5 ();
use STF::Constants qw(
    STF_DEBUG
    STORAGE_MODE_READ_WRITE
    STORAGE_CLUSTER_MODE_READ_WRITE
    STORAGE_CLUSTER_MODE_READ_ONLY
);
use STF::Log;
use STF::API::Storage;

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

    local $STF::Log::PREFIX = "Store(C)" if STF_DEBUG;

    my $cluster   = $args->{cluster}   or die "XXX no cluster";
    my $object_id = $args->{object_id} or die "XXX no object_id";
    my $content   = $args->{content}   or die "XXX no content";;
    my $minimum   = $args->{minimum};
    my $force     = $args->{force};
    my $repair    = $args->{repair};

    my $object    = $self->get('API::Object')->lookup($object_id);
    if (! $object) {
        debugf(
            "Could not load object to store (object_id = %s)n",
            $object_id,
        ) if STF_DEBUG;
        return;
    }

    # If we're being repaired, then it's ok to write to the "in repair"
    # storages, so we should be looking for all possible modes
    my $modes = $repair ?
        \@STF::API::Storage::WRITABLE_MODES_ON_REPAIR :
        \@STF::API::Storage::WRITABLE_MODES
    ;
    my @storages = $self->get('API::Storage')->search({
        cluster_id => $cluster->{id},
        mode       => { in => $modes },
    });
    if (@storages < 3) { # we MUST have at least 3 storages to write to
        if (STF_DEBUG) {
            debugf ("Cluster %s does not have enough storages to write to (minimum 3, got %d)", $cluster->{id}, scalar @storages);
        }
        return;
    }
    if (STF_DEBUG) {
        debugf(
            "Attempting to store object %s in cluster %s (want %d copies)",
            $object_id,
            $cluster->{id},
            defined $minimum ? $minimum : scalar @storages,
        );
        debugf("Going to store %s in:", $object_id);
        foreach my $storage (@storages) {
            debugf(" + [%s] %s (%s)", $storage->{id}, $storage->{uri}, $object_id);
        }
    }

    my $md5 = Digest::MD5->new;
    my $expected = $md5->addfile( $content )->hexdigest;
    my $entity_api = $self->get('API::Entity');
    my $stored = 0;
    # This object must be stored at least X times
    foreach my $storage (List::Util::shuffle(@storages)) {
        # if we can fetch it, don't store it
        my $fetched;

        # Without the $force flag, we fetch the object before storing to
        # avoid redundunt writes. $force should only be used when you KNOW
        # that this is a new entity
        if (! $force) {
            $fetched = $entity_api->fetch_content({
                object => $object,
                storage => $storage,
                repair => 1,
            });
        }
        if ($fetched) {
            if ($md5->new->addfile($fetched)->hexdigest eq $expected) {
                $stored++;
                debugf(
                    "Object %s already exist on storage %s",
                    $object_id, $storage->{id},
                ) if STF_DEBUG;

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
                repair  => $repair,
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


    debugf(
        "Stored %d entities in cluster %s (wanted %d)",
        $stored,
        $cluster->{id},
        defined $minimum ? $minimum : scalar @storages
    );
    return $ok;
}

sub check_entity_health {
    my ($self, $args) = @_;
    my $object_id  = $args->{object_id} or die "XXX no object";
    my $cluster_id = $args->{cluster_id} or die "XXX no cluster";
    my $repair     = $args->{repair};

    local $STF::Log::PREFIX = "Cluster" if STF_DEBUG;

    if (STF_DEBUG) {
        debugf( "Checking entity health for object %s on cluster %s", $object_id, $cluster_id );
    }

    # Short circuit. If the cluster mode is not rw or ro, then
    # we have a problem.
    my $cluster = $self->lookup($cluster_id);
    if ($cluster->{mode} != STORAGE_CLUSTER_MODE_READ_WRITE &&
        $cluster->{mode} != STORAGE_CLUSTER_MODE_READ_ONLY
    ) {
        if (STF_DEBUG) {
            debugf("Cluster %s is not read-write or read-only, need to move object %s out of this cluster", $cluster_id, $object_id );
        }
        return ();
    }

    my @storages = $self->get('API::Storage')->search({
        cluster_id => $cluster_id
    });
    if (! @storages) {
        debugf("Could not find any storages belonging to cluster %s", $cluster_id) if STF_DEBUG;
        return ();
    }

    # only check until the first failure
    my $entity_api = $self->get('API::Entity');
    foreach my $storage (List::Util::shuffle(@storages)) {
        my $ok = $entity_api->check_health({
            object_id  => $object_id,
            storage_id => $storage->{id},
            repair     => $repair,
        });
        if (! $ok) {
            return ();
        }
    }
    return 1;
}

sub register_for_object {
    my ($self, $args) = @_;

    local $STF::Log::PREFIX = "Cluster" if STF_DEBUG;

    my $object_id = $args->{object_id} or die "XXX no object";
    my $cluster_id = $args->{cluster_id} or die "XXX no cluster";

    debugf(
        "Registering object %s to cluster %s",
        $object_id,
        $cluster_id,
    ) if STF_DEBUG;

    my $dbh = $self->dbh;
    my $rv;
    eval {
        my $sth = $dbh->prepare(<<EOSQL)
            SELECT 1 FROM object WHERE id = ?
EOSQL
        $rv = $sth->execute($object_id);
        $sth->finish;
        if ($rv <= 0) {
            die "Object $object_id not found";
        }

        $rv = $dbh->do(<<EOSQL, undef, $object_id, $cluster_id);
            INSERT IGNORE INTO object_cluster_map (object_id, cluster_id) VALUES (?, ?)
EOSQL
    };
    if ($@) {
        critf("Error while registering object %s to cluster %s: %s", $object_id, $cluster_id, $@);
    }
    return $rv;
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
    debugf(
        "No cluster defined for object %s yet. Created mapping for cluster %d",
        $object_id, $cluster->{id},
    ) if STF_DEBUG;
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

