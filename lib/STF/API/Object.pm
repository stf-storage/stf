package STF::API::Object;
use Mouse;
use Digest::MurmurHash ();
use HTTP::Status ();
use List::Util ();
use Scope::Guard ();
use STF::Constants qw(
    :object
    :storage
    STF_DEBUG
    STF_TRACE
    STF_TIMER
    STF_ENABLE_OBJECT_META
);
use STF::Log;
use STF::Dispatcher::PSGI::HTTPException;
use STF::Utils ();

with 'STF::API::WithDBI';

has urandom => (
    is => 'rw',
    lazy => 1,
    builder => sub {
        String::Urandom->new( LENGTH => 30, CHARS => [ 'a' .. 'z' ] );
    }
);

sub lookup_meta {
    if ( STF_ENABLE_OBJECT_META ) {
        my ($self, $object_id) = @_;
        return $self->get('API::ObjectMeta')->lookup_for( $object_id );
    }
}

# XXX Used only for admin, so efficiency is ignore!
sub search_with_entity_info {
    my ($self, $where, $opts) = @_;
    my $s = $self->sql_maker->new_select;

    my $entity_api = $self->get('API::Entity');
    my @objects = $self->search( $where, $opts );
    foreach my $object (@objects) {
        $object->{entity_count} = $entity_api->count({ object_id => $object->{id } });
    }
    return wantarray ? @objects : \@objects;
}


sub create_internal_name {
    my ($self, $args) = @_;

    my $suffix = $args->{suffix} || 'dat';

    # create 30 characters long random a-z filename
    my $fname = $self->urandom->rand_string;

    if ( $fname !~ /^(.)(.)(.)(.)/ ) {
        die "PANIC: Can't parse file name for directories!";
    }

    File::Spec->catfile( $1, $2, $3, $4, "$fname.$suffix" );
}

sub find_active_object_id {
    my ($self, $args) = @_;
    $self->find_object_id( { %$args, status => OBJECT_ACTIVE } );
}

sub find_object_id {
    my ($self, $args) = @_;
    my ($bucket_id, $object_name) = @$args{ qw(bucket_id object_name) };
    my $dbh = $self->dbh;

    my $sql = <<EOSQL;
        SELECT id FROM object WHERE bucket_id = ? AND name = ?
EOSQL
    my @args = ($bucket_id, $object_name);
    if (exists $args->{status}) {
        push @args, $args->{status};
        $sql .= " AND status = ?";
    }

    my ($id) = $dbh->selectrow_array( $sql, undef, @args );
    return $id;
}

sub create {
    my ($self, $args) = @_;

    my ($object_id, $bucket_id, $object_name, $internal_name, $size, $replicas) =
        delete @$args{ qw(id bucket_id object_name internal_name size replicas) };

    my $dbh = $self->dbh;
    my $rv  = $dbh->do(<<EOSQL, undef, $object_id, $bucket_id, $object_name, $internal_name, $size, $replicas);
        INSERT INTO object (id, bucket_id, name, internal_name, size, num_replica, created_at) VALUES (?, ?, ?, ?, ?, ?, UNIX_TIMESTAMP(NOW()))
EOSQL

    return $rv;
}

# We used to use replicate() for both the initial write and the actual
# replication, but it has been separated out so that you can make sure
# that we do a double-write in the initial store(), and replication that
# happens afterwards runs with a different logic
sub store {
    my ($self, $args) = @_;

    local $STF::Log::PREFIX = "Store(O)";
    my $object_id     = $args->{id}            or die "XXX no id";
    my $bucket_id     = $args->{bucket_id}     or die "XXX no bucket_id";
    my $object_name   = $args->{object_name}   or die "XXX no object_name";
    my $input         = $args->{input}         or die "XXX no input";;
    my $size          = $args->{size} || 0;
    my $replicas      = $args->{replicas} || 3; # XXX unused, stored for compat
    my $suffix        = $args->{suffix} || 'dat';
    my $force         = $args->{force};
    my $cluster_api = $self->get('API::StorageCluster');

    # XXX If this fails because internal_name is not unique, try for at least 10 times
    my $create_ok = 0;
    for my $try (1..10) {
        my $internal_name = $self->create_internal_name( { suffix => $suffix } );
        eval {
            $self->create({
                id            => $object_id,
                bucket_id     => $bucket_id,
                object_name   => $object_name,
                internal_name => $internal_name,
                size          => $size,
                replicas      => $replicas, # Unused, stored for back compat
            });
            $create_ok = 1;
        };
        if (my $e = $@) {
            # XXX Retry only if internal_name is a conflict
            if ($e =~ /Duplicate entry '[^']+' for key 'internal_name'/) {
                if (STF_DEBUG) {
                    debugf("Object internal_name (%s) was a duplicate, trying again (#%d)", $internal_name, $try);
                }
                next;
            }
            Carp::croak($e);
        }
        last if $create_ok;
    }
    if (! $create_ok) {
        if (STF_DEBUG) {
            debugf("Failed to create a unique internal_name for object. Bailing out");
        }
        Carp::croak("Failed to create objet");
    }

    # After this point if something wicked happens and we bail out,
    # we don't want to keep this object laying around in a half-baked
    # state. So make sure to get rid of it
    my $guard = Scope::Guard->new(sub {
        if (STF_DEBUG) {
            debugf("Guard for API::Object->store($object_id) triggered");
        }
        eval { $self->delete( $object_id ) };
    });

    # Load all possible clusters, ordered by a consistent hash
    my @clusters = $cluster_api->load_candidates_for( $object_id );
    if (! @clusters) {
        critf(
            "No cluster defined for object %s, and could not any load cluster for it\n",
            $object_id
        );
        return;
    }

    # At this point we still don't know which cluster we belong to.
    # Attempt to write into clusters in order.
    foreach my $cluster (@clusters) {
        my $ok = $cluster_api->store({
            cluster   => $cluster,
            object_id => $object_id,
            content   => $input,
            minimum   => 2,
            force     => $force,
        });
        if ($ok) {
            $cluster_api->register_for_object( {
                cluster_id => $cluster->{id},
                object_id  => $object_id
            });
            # done
            $guard->dismiss;
            return 1;
        }
    }

    return;
}

sub repair {
    my ($self, $object_id)= @_;

    local $STF::Log::PREFIX = "Repair(O)";

    debugf("Repairing object %s", $object_id) if STF_DEBUG;
    my $timer;
    if (STF_TIMER) {
        $timer = STF::Utils::timer_guard();
    }

    my $object = $self->lookup( $object_id );
    my $entity_api = $self->get( 'API::Entity' );

    # XXX Object does not exist. Hmm, glitch? either way, there's no
    # way for anybody to get to this object, so we might as well say
    # goodbye to the entities that belong to this object, if any
    if (! $object) {
        debugf("No matching object %s", $object_id ) if STF_DEBUG;

        my @entities = $entity_api->search( {
            object_id => $object_id 
        } );
        if (! @entities) {
            return;
        }
        if ( STF_DEBUG ) {
            debugf("Removing orphaned entities in storages:\n");
            foreach my $entity ( @entities ) {
                debugf("+ %s", $entity->{storage_id});
            }
        }
        $entity_api->delete( {
            object_id => $object_id
        } );
        return;
    }

    # Attempt to read from any given resource
    my $master_content = $entity_api->fetch_content_from_any({
        object => $object,
        repair => 1,
    });
    if (! $master_content) {
        $master_content = $entity_api->fetch_content_from_all_storage({
            object => $object,
            repair => 1,
        });
        if (! $master_content) {
            critf(
                "PANIC: No content for %s could be fetched!! Cannot proceed with repair.",
                $object->{id}
            );
            return;
        }
    }

    my $cluster_api = $self->get( 'API::StorageCluster' );

    # find the current cluster
    my $cluster = $cluster_api->load_for_object( $object_id  );
    my @clusters = $cluster_api->load_candidates_for( $object_id );

    # The object should be in the first cluster found, so run a health check
    my $ok = $cluster_api->check_entity_health({
        cluster_id => $clusters[0]->{id},
        object_id  => $object_id,
        repair     => 1,
    });

    my $designated_cluster;
    if ($ok) {
        debugf(
            "Object %s is correctly stored in cluster %s. Object does not need repair",
            $object_id, $clusters[0]->{id}
        ) if STF_DEBUG;

        $designated_cluster = $clusters[0];
        if (! $cluster || $designated_cluster->{id} != $cluster->{id}) {
            $cluster_api->register_for_object({
                cluster_id => $designated_cluster->{id},
                object_id => $object_id,
            });
        }
    } else {
        debugf( "Object %s needs repair", $object_id ) if STF_DEBUG;
        # If it got here, either the object was not properly in clusters[0]
        # (i.e., some of the storages in the cluster did not have this object)
        # or it was in a different cluster
        foreach my $cluster ( @clusters ) {
            # The first one is where we should be, but there's always a chance
            # that it's broken, so we need to try all clusters.
            my $ok = $cluster_api->store({
                cluster   => $cluster,
                object_id => $object_id,
                content   => $master_content,
                repair    => 1,
            });
            if ($ok) {
                $designated_cluster = $cluster;
                last;
            }
        }

        if (! $designated_cluster) {
            critf("PANIC: Failed to repair object %s to any cluster!", $object_id);
            return;
        }
    }

    # Object is now properly stored in $designated_cluster. Find which storages
    # map to this, and remove any other. This may happen if we added new
    # clusters and rebalancing occurred.
    my $storage_api = $self->get('API::Storage');
    my @storages = $storage_api->search({
        cluster_id => { 'not in' => [ $designated_cluster->{id} ] },
    });
    my @entities = $entity_api->search({
        object_id => $object_id,
        storage_id => { in => [ map { $_->{id} } @storages ] }
    });
    if (STF_DEBUG) {
        debugf( "Object %s has %d entities that's not in cluster %d",
            $object_id, scalar @entities, $designated_cluster->{id});
    }
    my $cache_key = [ storages_for => $object_id ];
    my $guard;
    if (! $ok) {
        $guard = Scope::Guard->new(sub {
            if (STF_DEBUG) {
                debugf( "Invalidating cache %s", join ".", @$cache_key );
            }
            $self->cache_delete(@$cache_key);
        });
    }
    if (@entities) {
        if (STF_DEBUG) {
            debugf( "Extra entities found: dropping status flag, then proceeding to remove %d entities", scalar @entities );
        }
        # drop the status field before doing the actual removal
        foreach my $entity (@entities) {
            $entity_api->update(
                {
                    storage_id => $entity->{storage_id},
                    object_id  => $entity->{object_id},
                },
                {
                    status => 0,
                }
            );
        }

        # Make sure to invalidate the cache here, because we don't want
        # the dispatcher to pick the entities with status = 0
        undef $guard;

        $entity_api->remove({
            object   => $object,
            storages => [ map { $storage_api->lookup($_->{storage_id}) } @entities ],
        });
    }

    # $guard gets freed, and the cache gets invalidated, if it hasn't
    # already been released.
    return 1;
}

sub get_any_valid_entity_url {
    my ($self, $args) = @_;

    local $STF::Log::PREFIX = "Get Any(O)" if STF_DEBUG;

    my ($bucket_id, $object_name, $if_modified_since, $check) =
        @$args{ qw(bucket_id object_name if_modified_since health_check) };
    my $object_id = $self->find_active_object_id($args);
    if (! $object_id) {
        debugf(
            "Could not get object_id from bucket ID (%s) and object name (%s)",
            $bucket_id,
            $object_name
        ) if STF_DEBUG;
        return;
    }

    my $object = $self->lookup( $object_id );

    # XXX We have to do this before we check the entities, because in real-life
    # applications many of the requests come with an IMS header -- which 
    # short circuits from this method, and never allows us to reach this
    # enqueuing condition
    if ($check) {
        debugf(
            "Object %s forcefully being sent to repair (probably harmless)",
            $object_id
        ) if STF_DEBUG;
        eval {
            my $memd = $self->get('Memcached');
            if (! $memd->get("repair_from_dispatcher.$object_id")) {
                $self->get('API::Queue')->enqueue( repair_object => $object_id );
                $memd->add("repair_from_dispatcher.$object_id", 1, 300);
            }
        };
    }

    # We cache
    #   "storages_for.$object_id => [
    #       [ $storage_id, $storage_uri ],
    #       [ $storage_id, $storage_uri ],
    #       [ $storage_id, $storage_uri ],
    #       ...
    #   ]
    my $repair = 0;
    my $cache_key = [ storages_for => $object_id ];
    my $storages = $self->cache_get( @$cache_key );
    if ($storages) {
        if (STF_DEBUG) {
            debugf( "Cache HIT for storages (object_id = %s)", $object_id );
        }

        # Got storages, but we need to validate that they are indeed
        # readable, and that the uris match
        my @storage_ids = map { $_->[0] } @$storages;
        my $storage_api = $self->get('API::Storage');
        my $lookup      = $storage_api->lookup_multi( @storage_ids );

        # If *any* of the storages fail, we should re-compute
        foreach my $storage_id ( @storage_ids ) {
            my $storage = $lookup->{ $storage_id };
            if (! $storage || ! $storage_api->is_readable( $storage ) ) {
                # Invalidate the cached entry
                undef $storages;

                # If this storage is just DOWN, then it's a temporary problem.
                # we don't need to repair it. Hopefully it will come back up soon
                if ($storage->{mode} == STORAGE_MODE_TEMPORARILY_DOWN) {
                    if (STF_DEBUG) {
                        debugf("Storage '%s' is down. Invalidating cache, but NOT triggering a repair", $storage_id);
                    }
                } else {
                    # Otherwise, by all means please repair this object
                    $repair++;
                    if (STF_DEBUG) {
                        debugf( "Storage '%s' is not readable anymore. Invalidating cache", $storage_id);
                    }
                }

                if (STF_TRACE) {
                    $self->get('Trace')->trace( "stf.object.get_any_valid_entity_url.invalidated_storage_cache");
                }
                last;
            }
        }

        if (STF_DEBUG) {
            debugf( "Cached storages for object_id = %s are %s",
                $object_id, $storages ? "OK" : "NOT OK");
        }
    } 

    if (! $storages) {
        my $dbh = $self->dbh('DB::Master');
        my $sth = $dbh->prepare(<<EOSQL);
            SELECT s.id, s.uri
            FROM object o JOIN entity e ON o.id = e.object_id
                          JOIN storage s ON s.id = e.storage_id 
            WHERE
                o.id = ? AND
                o.status = 1 AND 
                e.status = 1 AND
                s.mode IN ( ?, ? )
EOSQL

        my $rv = $sth->execute($object_id, STORAGE_MODE_READ_ONLY, STORAGE_MODE_READ_WRITE);

        my ($storage_id, $uri);
        $sth->bind_columns(\($storage_id, $uri));

        my %storages;
        while ( $sth->fetchrow_arrayref ) {
            $storages{$storage_id} = $uri;
        }
        $sth->finish;

        my %h = map {
            ( $_ => Digest::MurmurHash::murmur_hash("$storages{$_}/$object->{internal_name}") )
        } keys %storages;
        $storages = [
            map  { [ $_, $storages{$_} ] } 
            sort { $h{$a} <=> $h{$b} }
            keys %h
        ];

        $self->cache_set( $cache_key, $storages, $self->cache_expires );
    }

    if (STF_DEBUG) {
        debugf("Backend storage candidates:");
        foreach my $storage ( @$storages ) {
            debugf("    * [%s] %s", $storage->[0], $storage->[1]);
        }
    }

    # Send successive HEAD requests
    my $fastest;
    my $furl = $self->get('Furl');
    my $headers;
    if ( $if_modified_since ) {
        $headers = [ 'If-Modified-Since' => $if_modified_since ];
    }

    foreach my $storage ( @$storages ) {
        my $url = "$storage->[1]/$object->{internal_name}";
        debugf("Sending HEAD %s", $url) if STF_DEBUG;

        local $furl->{timeout} = 5;
        my (undef, $code) = $furl->head( $url, $headers );

        debugf(
            "        HEAD %s was %s (%s)",
            $url,
            $code =~ /^[23]/ ? "OK" : "FAIL",
            $code
        ) if STF_DEBUG;

        if ( HTTP::Status::is_success( $code ) ) {
            $fastest = $url;
            last;
        } elsif ( HTTP::Status::HTTP_NOT_MODIFIED() == $code ) {
            # XXX if this is was not modified, then short circuit
            debugf(
                "IMS request to %s returned NOT MODIFIED. Short-circuiting",
                $object_id
            ) if STF_DEBUG;
            STF::Dispatcher::PSGI::HTTPException->throw( 304, [], [] );
        } else {
            # What?! the request FAILED? Need to repair this sucker.
            $repair++;
        }
    };

    if ($repair) { # Whoa!
        my $memd = $self->get('Memcached');
        if (! $memd->get("repair_from_dispatcher.$object_id")) {
            debugf("Object %s needs repair", $object_id) if STF_DEBUG;
            eval {
                $self->get('API::Queue')->enqueue( repair_object => $object_id );
                $memd->add("repair_from_dispatcher.$object_id", 1, 300);
                # Also, kill the cache
                $self->cache_delete( @$cache_key );
            };
        }
    }

    if ( STF_DEBUG ) {
        if (! $fastest) {
            critf("All HEAD requests failed");
        } else {
            debugf(
                "HEAD request to %s was fastest (object_id = %s)",
                $fastest,
                $object_id,
            );
        }
    }

    return $fastest || ();
}

# Set this object_id to be deleted. Deletes the object itself, but does
# not delete the entities
sub mark_for_delete {
    my ($self, $object_id) = @_;

    local $STF::Log::PREFIX = "MarkDel(O)" if STF_DEBUG;

    my $dbh = $self->dbh;

    # Do a SELECT / then an insert
    my $select_sth = $dbh->prepare("SELECT 1 FROM deleted_object WHERE id = ?")
    my $rv_select = $select_sth->execute($object_id);
    $select_sth->finish;
    if ($rv_select > 0) { # FOUND
        # Do a REPLACE
        my $rv = $dbh->do( <<EOSQL, undef, $object_id );
            REPLACE INTO deleted_object SELECT * FROM object WHERE id = ?
EOSQL
        if ( $rv <= 0 ) {
            if (STF_DEBUG) {
                debugf(
                    "Failed to replace object %s into deleted_object (rv = %s)",
                    $object_id, $rv
                );
            }
            return; # Failure to REPLACE
        }

        if (STF_DEBUG) {
            debugf(
                "Replaced object %s into deleted_object (rv = %s)",
                $object_id,
                $rv,
            );
        }
    } else {
        # object was not found. Insert it. if insert fails, it just measn
        # that somebody just inserted it before us, so it's OK

        # We temporarily suspend raising errors here, because we can and
        # want to ignore this INSERT error
        local $dbh->{RaiseError} = 0;
        my $rv = $dbh->do(<<EOSQL, undef, $object_id)
            INSERT INTO deleted_object SELECT * FROM object WHERE id = ?
EOSQL

        # If the insert was a failure, just return this particular
        # request as fail. it's okay if somebody else has already
        # inserted it before us
        if ($rv <= 0){
            if (STF_DEBUG) {
                debugf("Failed to insert object %s into deleted_object (rv = %s)",
                    $object_id,
                    $rv,
                );
            }
            return;
        }

        if (STF_DEBUG) {
            debugf(
                "Inserted object %s into deleted_object (rv = %s)",
                $object_id,
                $rv,
            );
        }
    }

    my $rv_delete = $dbh->do( <<EOSQL, undef, $object_id );
        DELETE FROM object WHERE id = ?
EOSQL
    if ($rv_delete <= 0) {
        if (STF_DEBUG) {
            debugf("Failed to delete object %s from object (rv = %s)",
                $object_id,
                $rv_delete,
            );
        }
    }

    if (STF_DEBUG) {
        debugf(
            "Deleted object %s from object (rv = %s)",
            $object_id,
            $rv_delete
        );
    }

    return 1;
}

sub rename {
    my ($self, $args) = @_;

    local $STF::Log::PREFIX = "Rename(O)";
    my $source_bucket_id = $args->{ source_bucket_id };
    my $source_object_name = $args->{ source_object_name };
    my $dest_bucket_id = $args->{ destination_bucket_id };
    my $dest_object_name = $args->{ destination_object_name };

    my $source_object_id = $self->find_object_id( {
        bucket_id =>  $source_bucket_id,
        object_name => $source_object_name
    } );

    # This should always exist
    if (! $source_object_id ) {
        critf(
            "Source object did not exist (bucket_id = %s, object_name = %s)",
            $source_bucket_id, $source_object_name
        );
        return;
    }

    # This shouldn't exist
    my $dest_object_id = $self->find_object_id( {
        bucket_id => $dest_bucket_id,
        object_name => $dest_object_name
    } );
    if ( $dest_object_id ) {
        critf(
            "Destination object already exists (bucket_id = %s, object_name = %s)",
            $dest_bucket_id, $dest_object_name
        );
        return;
    }

    $self->update( $source_object_id, {
        bucket_id => $dest_bucket_id,
        name      => $dest_object_name
    } );
}

# When we receive ->cache_delete( $self->table, $id ), then we should
# be deleting some other caches as well
around cache_delete => sub {
    my ($next, $self, @args) = @_;

    $self->$next(@args);
    if (@args == 2 && $args[0] eq $self->table) {
        debugf( "Cache delete 'storages_for.%s', too", $args[1] );
        $self->cache_delete( "storages_for", $args[1] );
    }
};

no Mouse;

1;
