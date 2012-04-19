package STF::API::Object;
use Mouse;
use Digest::MurmurHash ();
use HTTP::Status ();
use STF::Constants qw(
    :object
    STF_DEBUG
    STF_ENABLE_OBJECT_META
    STORAGE_MODE_TEMPORARILY_DOWN
    STORAGE_MODE_READ_ONLY
    STORAGE_MODE_READ_WRITE
    STORAGE_MODE_SPARE
);
use STF::Dispatcher::PSGI::HTTPException;

with 'STF::API::WithDBI';

has urandom => (
    is => 'rw',
    lazy => 1,
    builder => sub {
        String::Urandom->new( LENGTH => 30, CHARS => [ 'a' .. 'z' ] );
    }
);

has max_num_replica => (
    is => 'rw',
);

has min_num_replica => (
    is => 'rw',
);

sub lookup_meta {
    if ( STF_ENABLE_OBJECT_META ) {
        my ($self, $object_id) = @_;
        return $self->get('API::ObjectMeta')->lookup_for( $object_id );
    }
}

sub status_for {
    my ($self, $id) = @_;

    my $object = $self->find( $id );
    if (! $object ) {
        return (); # no object;
    }

    my @entities = $self->get( 'API::Entity' )->search( {
        object_id => $id
    } );
    $object->{entities} = @entities;
    return $object;
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

sub load_objects_since {
    my ($self, $object_id, $limit) = @_;
    my $dbh = $self->dbh('DB::Master');
    my $results = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, $object_id, $limit );
        SELECT * FROM object WHERE id > ? LIMIT ?
EOSQL
    return wantarray ? @$results : $results;
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

sub find_neighbors {
    my ($self, $object_id, $breadth) = @_;

    if ($breadth <= 0) {
        $breadth = 10;
    }

    my $dbh = $self->dbh;
    # find neighbors (+/- $breadth items)
    my $before = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, $object_id );
        SELECT * FROM object WHERE id < ? ORDER BY id DESC LIMIT $breadth 
EOSQL
    my $after = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, $object_id );
        SELECT * FROM object WHERE id > ? ORDER BY id ASC LIMIT $breadth 
EOSQL

    return (@$before, @$after)
}

sub find_suspicious_neighbors {
    my ($self, $args) = @_;

    my $object_id = $args->{object_id} or die "XXX no object_id";
    my $storages  = $args->{storages}  or die "XXX no storages";
    my $breadth   = $args->{breadth} || 0;

    if ($breadth <= 0) {
        $breadth = 10;
    }

    my %objects;
    my $dbh = $self->dbh;
    foreach my $storage_id ( @$storages ) {
        # find neighbors in this storage
        my $before = $dbh->selectall_arrayref( <<EOSQL, undef, $storage_id, $object_id );
            SELECT e.object_id FROM entity e FORCE INDEX (PRIMARY)
                WHERE e.storage_id = ? AND e.object_id < ?
                ORDER BY e.object_id DESC LIMIT $breadth
EOSQL
        my $after = $dbh->selectall_arrayref( <<EOSQL, undef, $storage_id, $object_id );
            SELECT e.object_id FROM entity e FORCE INDEX (PRIMARY)
                WHERE e.storage_id = ? AND e.object_id > ?
                ORDER BY e.object_id ASC LIMIT $breadth
EOSQL

        foreach my $row ( @$before, @$after ) {
            my $object_id = $row->[0];
            next if $objects{ $object_id };

            my $object = $self->lookup( $object_id );
            if ($object) {
                $objects{ $object_id } = $object;
            }
        }
    }

    return values %objects;
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

# Returns good/bad entities -- actually, returns the storages that contains
# the broken entities.
# my ($valid_listref, $invalid_listref) = $api->check_health($object_id);
sub check_health {
    my ($self, $object_id) = @_;

    if (STF_DEBUG) {
        print STDERR "[    Health] Checking health for object $object_id\n";
    }

    my $object = $self->lookup( $object_id );
    if (! $object) {
        if (STF_DEBUG) {
            print STDERR "[    Health] No matching object $object_id\n";
        }
        return ([], []);
    }

    my $object_meta = $self->lookup_meta( $object_id );
    if (! $object_meta) {
        if (STF_DEBUG) {
            print STDERR "[    Health] No matching object meta for $object_id (harmless)\n";
        }
    }

    my $entity_api = $self->get( 'API::Entity' );
    my @entities = $entity_api->search( { object_id => $object_id } );
    if (@entities) {
        if (STF_DEBUG) {
            printf STDERR "[    Health] Loaded %d entities\n",
                scalar @entities
        }
    } else {
        if (STF_DEBUG) {
            print STDERR "[    Health] No matching entities for $object_id ( XXX broken ? )\n";
        }
        return ([], []);
    }

    my $storage_api = $self->get('API::Storage');
    my $furl = $self->get('Furl');
    my $ref_url;
    my ($content, @intact, @broken);
    foreach my $entity ( @entities ) {
        my $storage = $storage_api->lookup( $entity->{storage_id} );
        if (! $storage) {
            if (STF_DEBUG) {
                print STDERR "[    Health] storage $entity->{storage_id} does not exist. Adding to broken list\n";
            }
            push @broken, { id => $entity->{storage_id} };
            next;
        }

        # an entity in TEMPORARILY_DOWN node needs to be treated as alive
        if ($storage->{mode} == STORAGE_MODE_TEMPORARILY_DOWN) {
            if (STF_DEBUG) {
                print STDERR "[    Health] storage $entity->{storage_id} is temporarily down. Adding to intact list\n";
            }
            push @intact, $storage;
            next;
        }

        # If the mode is not in a readable state, then we've purposely 
        # taken it out of the system, and needs to be repaired. Also, 
        # if this were the case, we DO NOT issue an DELETE on the backend, 
        # as it most likely will not properly respond.
        if ($storage->{mode} != STORAGE_MODE_READ_ONLY && $storage->{mode} != STORAGE_MODE_READ_WRITE) {
            if (STF_DEBUG) {
                print STDERR "[    Health] Storage $storage->{id} is not readable. Adding to invalid list.\n";
            }

            push @broken, $storage;

            # This "next" by-passes the HEAD request that we'd normally
            # send to the storage.
            next;
        }

        my $url = join "/", $storage->{uri}, $object->{internal_name};
        if (STF_DEBUG) {
            print STDERR "[    Health] Going to check $url\n";
        }

        my $fh = File::Temp->new( UNLINK => 1 );
        my (undef, $code) = eval {
            $furl->request(
                url => $url,
                method => "GET",
                write_file => $fh,
            );
        };
        if ($@) {
            print STDERR "[    Health] HTTP request raised an exception: $@\n";
            # Make sure this becomes an error
            $code = 500;
        }
        my $is_success = HTTP::Status::is_success( $code );
        if (STF_DEBUG) {
            printf STDERR "[    Health] GET %s was %s (%d)\n",
                $url, ($is_success ? "OK" : "FAIL"), $code;
        }

        $fh->flush;
        $fh->seek(0, 0);
        my $size = (stat($fh))[7];
        if ( $size != $object->{size} ) {
            $is_success = 0;
            if ( STF_DEBUG ) {
                printf STDERR "[    Health] Object %s sizes do not match (got %d, expected %d)\n",
                    $object->{id},
                    $size,
                    $object->{size}
                ;
            }
        }

        if (STF_ENABLE_OBJECT_META && $object_meta && $is_success) {
            # XXX If the object wasn't created with meta info (which can
            # happen), then we shouldn't run all this
            my $hash = Digest::MD5->new;
            my $hash_ok = 0;
            $hash->addfile( $fh );
            my $expected = $object_meta->{hash};
            my $actual   = $hash->hexdigest();
            $hash_ok = $expected eq $actual;
            if (STF_DEBUG) {
                printf STDERR "[    Health] MD5 hashes for %s %s (DB = %s, actual = %s)\n",
                    $url, ( $hash_ok ? "OK" : "FAIL"), $expected, $actual
                ;
            }

            $is_success = $hash_ok;
        }

        if ($is_success) {
            $ref_url ||= $url;
            push @intact, $storage;
        } else {
            push @broken, $storage;
        }
    }

    if ( STF_DEBUG ) {
        foreach my $storage (@intact) {
            printf "[    Health] + OK $storage->{uri}/$object->{internal_name}\n";
        }
        foreach my $storage (@broken) {
            printf "[    Health] - NOT OK $storage->{uri}/$object->{internal_name}\n";
        }
    }

    return (\@intact, \@broken);
}

sub repair {
    my ($self, $object_id)= @_;

    if (STF_DEBUG) {
        print STDERR "[    Repair] Repairing object $object_id\n";
    }

    my $object = $self->lookup( $object_id );
    my $entity_api = $self->get( 'API::Entity' );
    if (! $object) {
        if (STF_DEBUG) {
            print STDERR "[    Repair] No matching object $object_id\n";
        }

        my @entities = $entity_api->search( {
            object_id => $object_id 
        } );
        if (@entities) {
            if ( STF_DEBUG ) {
                print STDERR "[    Repair] Removing orphaned entities in storages:\n";
                foreach my $entity ( @entities ) {
                    printf STDERR "[    Repair] + %s\n",
                        $entity->{storage_id}
                    ;
                }
            }
            $entity_api->delete( {
                object_id => $object_id
            } );
        }
        return;
    }

    my $cluster_api = $self->get( 'API::StorageCluster' );
    my $cluster = $cluster_api->load_for_object( $object->{id}, 1 );
    if (! $cluster) {
        if ( STF_DEBUG ) {
            printf STDERR "[    Repair] Could not load cluster for object %s, bailing out of repair\n",
                $object_id
            ;
        }
        return;
    }

    # XXX select all entities from our EXPECTED cluster.
    # if anything in there is broken, then attempt to load a good content
    # from any entity that looks like worth it (even if it's from a different
    # cluster).
    # once we have a good content, we should write to 

    my $storage_api = $self->get( 'API::Storage' );
    my @in_cluster = $storage_api->search( {
        cluster_id => $cluster->{id}
    });

    if ( STF_DEBUG ) {
        printf STDERR "[    Repair] Object %s should be in storages [%s]\n",
            $object->{id},
            join ", ", map { $_->{id} } @in_cluster
        ;
    }

    my (@broken, $master_content);
    foreach my $storage ( @in_cluster ) {
        my $content;

        if ( $storage->{mode} == STORAGE_MODE_READ_ONLY ||
            $storage->{mode} == STORAGE_MODE_READ_WRITE ||
            $storage->{mode} == STORAGE_MODE_SPARE )
        {
            $content = $entity_api->fetch_content({
                object  => $object,
                storage => $storage
            });
        }

        if (! $content) { # eek
            push @broken, $storage;
            if ( STF_DEBUG ) {
                printf "[    Repair] Failed to retrieve obejct %s from storage [%s]. Marking it as broken\n",
                    $object->{id},
                    $storage->{id}
                ;
            }
        } elsif ( ! $master_content ) {
            if ( STF_DEBUG ) {
                printf STDERR "[    Repair] Going to use content from storage %s (%s)\n",
                    $storage->{id},
                    "$storage->{uri}/$object->{internal_name}"
                ;
            }
            $master_content = $content;
        }
    }

    if (! $master_content) {
        if ( STF_DEBUG ) {
            printf STDERR "[    Repair] No content for %s could be fetched from storages in cluster %s. Falling back to reading from ANY source\n",
                $object->{id},
                $cluster->{id},
            ;
        }

        # Last resort. See if we can fetch from other sources
        $master_content = $entity_api->fetch_content_from_any({
            object => $object,
        });
    }

    if (! $master_content) {
        if ( STF_DEBUG ) {
            printf STDERR "[    Repair] PANIC: No content for %s could be fetched!! Cannot proceed with repair.\n",
                $object->{id}
            ;
        }
        return;
    }

    my $repaired = 0;
    if (@broken) { # we got something broken
        # write to all broken storages
        foreach my $storage ( @broken ) {
            if ( STF_DEBUG ) {
                printf STDERR "[    Repair] Repairing object %s on storage %s (%s)\n",
                    $object->{id},
                    $storage->{id},
                    "$storage->{uri}/$object->{internal_name}"
                ;
            }

            my $ok = $entity_api->store({
                storage => $storage,
                object  => $object,
                content => $master_content,
            });
            if ( STF_DEBUG ) {
                printf STDERR "[    Repair] STORE object %s to storage %s (mode = %s) was %s\n",
                    $object->{id},
                    $storage->{id},
                    $storage->{mode}, # XXX make it human-readable
                    $ok ? "OK" : "FAIL"
                ;
            }
            if ( $ok ) {
                $repaired++;
            }
        }

        if (@broken < $repaired) {
            # yikes, if we got here, that means we were able to get
            # some content out of the system, but we were not able to
            # properly store all copies. We shouldn't go to the next
            # step, which is to delete all entities that are NOT in
            # the expected cluster

            if ( STF_DEBUG ) {
                printf STDERR "[    Repair] Stored %d entities, but we started with %d broken entities, this is suspicious...\n",
                    $repaired,
                    scalar @broken
                ;
            }
            return;
        }

        # if we got here, cool, we should be at least as good as
        # the previous state
    }

    # before doing deletes, make sure that we have at least 2 entities
    # otherwise we shouldn't be performing deletes
    my @entities = $entity_api->search({
        object_id => $object->{id},
        storage_id => { in => [ map { $_->{id} } @in_cluster ] }
    } );
    if (@entities < 2) {
        if ( STF_DEBUG ) {
            printf STDERR "[    Repair] PANIC: We only have %d entities in cluster %s, AFTER attempting to repair!! (%d storages in cluster). Do you have enough storages in cluster (at least 2, recommend 3)? Is your storage having hardware issues?\n",
                scalar @entities,
                $cluster->{id},
                scalar @in_cluster,
            ;
        }
        return;
    }

    # now check if we have entities outside of our cluster.
    my @not_in_cluster = $entity_api->search({
        object_id => $object->{id},
        storage_id => { 'not in' => [ map { $_->{id} } @in_cluster ] }
    });
    if (@not_in_cluster > 0) {
        if ( STF_DEBUG ) {
            printf STDERR "[    Repair] Found storages in different cluster(s), deleting from %s\n",
                join ", ", map { $_->{storage_id} } @not_in_cluster;
        }
        $entity_api->remove_from({
            object => $object,
            storages => [ 
                grep { defined $_ }
                map { $storage_api->lookup( $_->{storage_id} ) }
                    @not_in_cluster
            ]
        });
    }
    $repaired += scalar @not_in_cluster;

    return wantarray ? ( $repaired, [ @broken, @not_in_cluster ] ) : $repaired;
}

sub get_any_valid_entity_url {
    my ($self, $args) = @_;

    my ($bucket_id, $object_name, $if_modified_since, $check) =
        @$args{ qw(bucket_id object_name if_modified_since health_check) };
    my $object_id = $self->find_active_object_id($args);
    if (! $object_id) {
        if (STF_DEBUG) {
            printf STDERR "[Get Entity] Could not get object_id from bucket ID (%s) and object name (%s)\n",
                $bucket_id,
                $object_name
            ;
        }
        return;
    }

    my $object = $self->lookup( $object_id );

    # XXX We have to do this before we check the entities, because in real-life
    # applications many of the requests come with an IMS header -- which 
    # short circuits from this method, and never allows us to reach this
    # enqueuing condition
    if ($check) {
        if ( STF_DEBUG ) {
            printf STDERR "[Get Entity] Object %s being sent to health check\n",
                $object_id
            ;
        }
        eval { $self->get('API::Queue')->enqueue( object_health => $object_id ) };
    }

    # We cache
    #   "storages_for.$object_id => {
    #       $storage_id, $storage_uri ],
    #       $storage_id, $storage_uri ],
    #       $storage_id, $storage_uri ],
    #       ...
    #   ]
    my $repair = 0;
    my $cache_key = [ storages_for => $object_id ];
    my $storages = $self->cache_get( @$cache_key );
    if ($storages) {
        # Got storages, but we need to validate that they are indeed
        # readable, and that the uris match
        my @storage_ids = grep { $_->[0] } @$storages;
        my $storage_api = $self->get('API::Storage');
        my $lookup      = $storage_api->lookup_multi( @storage_ids );

        # If *any* of the storages fail, we should re-compute
        foreach my $storage_id ( @storage_ids ) {
            if (! $lookup->{ $storage_id } ) {
                # Invalidate the cached entry, and set the repair flag
                undef $storages;
                $repair++;
                last;
            }
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

        if ( STF_DEBUG ) {
            print STDERR "[Get Entity] Backend storage candidates:\n";
            foreach my $storage ( @$storages ) {
                printf STDERR "[Get Entity] + [%s] %s\n",
                    $storage->[0],
                    $storage->[1]
                ;
            }
        }

        $self->cache_set( $cache_key, $storages, $self->cache_expires );
    }

    # XXX repair shouldn't be triggered by entities < num_replica
    #
    # We used to put the object in repair if entities < num_replica, but
    # in hindsight this was bad mistake. Suppose we mistakenly set
    # num_replica > # of storages (say you have 3 storages, but you
    # specified 5 replicas). In this case regardless of how many times we
    # try to repair the object, we cannot create enough replicas to
    # satisfy this condition.
    #
    # So that check is off. Let ObjectHealth worker handle it once
    # in a while.

    # Send successive HEAD requests
    my $fastest;
    my $furl = $self->get('Furl');
    my $headers;
    if ( $if_modified_since ) {
        $headers = [ 'If-Modified-Since' => $if_modified_since ];
    }

    foreach my $storage ( @$storages ) {
        my $url = "$storage->[1]/$object->{internal_name}";
        my (undef, $code) = $furl->head( $url, $headers );
        if ( HTTP::Status::is_success( $code ) ) {
            if ( STF_DEBUG ) {
                print STDERR "[Get Entity] + HEAD $url OK\n";
            }
            $fastest = $url;
            last;
        } elsif ( HTTP::Status::HTTP_NOT_MODIFIED() == $code ) {
            # XXX if this is was not modified, then short circuit
            if ( STF_DEBUG ) {
                printf STDERR "[Get Entity] IMS request to %s returned NOT MODIFIED. Short-circuiting\n",
                    $object_id
                ;
            }
            STF::Dispatcher::PSGI::HTTPException->throw( 304, [], [] );
        } else {
            if ( STF_DEBUG ) {
                print STDERR "[Get Entity] + HEAD $url failed: $code\n";
            }
            $repair++;
        }
    };

    if ($repair) { # Whoa!
        if ( STF_DEBUG ) {
            printf STDERR "[Get Entity] Object %s needs repair\n",
                $object_id
            ;
        }

        eval { $self->get('API::Queue')->enqueue( repair_object => $object_id ) };

        # Also, kill the cache
        eval { $self->cache_delete( @$cache_key ) };
    }

    if ( STF_DEBUG ) {
        if (! $fastest) {
            print STDERR "[Get Entity] All HEAD requests failed\n";
        } else {
            print STDERR "[Get Entity] HEAD request to $fastest was fastest\n";
        }
    }

    return $fastest || ();
}

# Set this object_id to be deleted. Deletes the object itself, but does
# not delete the entities
sub mark_for_delete {
    my ($self, $object_id) = @_;

    my $dbh = $self->dbh;
    my ($rv_replace, $rv_delete);

    $rv_replace = $dbh->do( <<EOSQL, undef, $object_id );
        REPLACE INTO deleted_object SELECT * FROM object WHERE id = ?
EOSQL

    if ( $rv_replace <= 0 ) {
        if ( STF_DEBUG ) {
            printf STDERR "[  Mark Del] Failed to insert object %s into deleted_object (rv = %s)\n",
                $object_id,
                $rv_replace
        }
    } else {
        if ( STF_DEBUG ) {
            printf STDERR "[  Mark Del] Inserted object %s into deleted_object (rv = %s)\n",
                $object_id,
                $rv_replace
            ;
        }

        $rv_delete = $dbh->do( <<EOSQL, undef, $object_id );
            DELETE FROM object WHERE id = ?
EOSQL

        if ( STF_DEBUG ) {
            printf STDERR "[  Mark Del] Deleted object %s from object (rv = %s)\n",
                $object_id,
                $rv_delete
            ;
        }
    }

    return $rv_replace && $rv_delete;
}

sub rename {
    my ($self, $args) = @_;

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
        if ( STF_DEBUG ) {
            printf STDERR "[    Rename] Source object did not exist (bucket_id = %s, object_name = %s)\n",
                $source_bucket_id,
                $source_object_name
            ;
        }
        return;
    }

    # This shouldn't exist
    my $dest_object_id = $self->find_object_id( {
        bucket_id => $dest_bucket_id,
        object_name => $dest_object_name
    } );
    if ( $dest_object_id ) {
        if ( STF_DEBUG ) {
            printf STDERR "[    Rename] Destination object already exists (bucket_id = %s, object_name = %s)\n",
                $dest_bucket_id,
                $dest_object_name
            ;
        }
        return;
    }

    $self->update( $source_object_id, {
        bucket_id => $dest_bucket_id,
        name      => $dest_object_name
    } );
}

no Mouse;

1;
