package STF::API::Object;
use strict;
use parent qw(STF::API::WithDBI);
use Digest::MurmurHash ();
use HTTP::Status ();
use STF::Constants qw(STF_DEBUG :object STORAGE_MODE_TEMPORARILY_DOWN STORAGE_MODE_READ_ONLY STORAGE_MODE_READ_WRITE);
use STF::Dispatcher::PSGI::HTTPException;
use Class::Accessor::Lite
    new => 1,
    rw => [ qw(furl urandom max_num_replica) ]
;

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

sub create {
    my ($self, $args) = @_;

    my ($object_id, $bucket_id, $object_name, $internal_name, $size, $replicas) =
        delete @$args{ qw(id bucket_id object_name internal_name size replicas) };

    my $dbh = $self->dbh;
    my $rv  = $dbh->do(<<EOSQL, undef, $object_id, $bucket_id, $object_name, $internal_name, $size, $replicas);
        INSERT INTO object (id, bucket_id, name, internal_name, size, num_replica, created_at) VALUES (?, ?, ?, ?, ?, ?, UNIX_TIMESTAMP(NOW()))
EOSQL
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

        my (undef, $code) = eval { $furl->head( $url ) };
        if ($@) {
            print STDERR "[    Health] HTTP request raised an exception: $@\n";
            # Make sure this becomes an error
            $code = 500;
        }

        my $is_success = HTTP::Status::is_success( $code );
        if (STF_DEBUG) {
            printf STDERR "[    Health] HEAD %s was %s (%d)\n",
                $url, ($is_success ? "OK" : "FAIL"), $code;
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
    if (! $object) {
        if (STF_DEBUG) {
            print STDERR "[    Repair] No matching object $object_id\n";
        }
        return;
    }

    my $entity_api = $self->get( 'API::Entity' );
    my @entities = $entity_api->search( { object_id => $object_id } );
    my ($intact, $broken) = $self->check_health( $object_id );

    if (! @$intact) {
        printf STDERR "[    Repair] No entities available, object %s is COMPLETELY BROKEN\n", $object_id;
        $self->update( $object_id => { status => OBJECT_INACTIVE } );
        return;
    }

    if ( @$broken ) {
        if (STF_DEBUG) {
            printf STDERR "[    Repair] Removing entities for $object_id in\n";
            foreach my $storage (@$broken) {
                print STDERR "[    Repair] + @{[ $storage->{uri} || '(null)' ]} (id = $storage->{id})\n";
            }
        }
        $entity_api->delete( {
            storage_id => [ -in => map { $_->{id} } @$broken ],
            object_id => $object_id
        } );
    }

    my $have = scalar @$intact;
    my $need = $object->{num_replica};
    my $max_num_replica = $self->max_num_replica;
    if ( defined $max_num_replica && $max_num_replica <= $have ) {
        if ( STF_DEBUG ) {
            printf STDERR "[    Repair] No need to repair %s (need %d, have %d, system max replica %d)\n",
                $object_id,
                $need,
                $have,
                $max_num_replica,
            ;
        }

        # Return the number of object fixed... which is nothing
        return 0;
    } elsif ($need <= $have) {
        if (STF_DEBUG) {
            printf STDERR "[    Repair] No need to repair %s (need %d, have %d)\n",
                $object_id,
                $need,
                $have,
            ;
        }

        # Return the number of object fixed... which is nothing
        return 0;
    } else {
        my $n = $need - $have;
        my $ref_url = join "/", $intact->[0]->{uri}, $object->{internal_name};
        if (STF_DEBUG) {
            printf STDERR "[    Repair] Going to replicated %s %d times\n",
                $object_id,
                $n,
            ;
            print STDERR "[    Repair] Using content from $ref_url\n";
        }
        my $furl = $self->get('Furl');
        my (undef, $code, undef, undef, $content) = $furl->get( $ref_url );
        if (! HTTP::Status::is_success( $code )) {
            die "PANIC: failed to retrieve supposedly good url $ref_url: $code";
        }

        my $replicated = $entity_api->replicate( {
            object_id => $object_id,
            content   => $content,
            replicas  => $n,
        } );

        if ( STF_DEBUG ) {
            printf STDERR "[    Repair] Wanted %d, replicated %d times\n",
                $n,
                $replicated,
            ;
        }
                
        # Return the number of object fixed... which is $replicated
        return $replicated;
    }
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



    my $entities = $self->cache_get( 'entities_for', $object_id );
    if (! $entities) {
        my $dbh = $self->dbh('DB::Master');
        my $sth = $dbh->prepare(<<EOSQL);
            SELECT s.uri, o.internal_name
            FROM object o JOIN entity e ON o.id = e.object_id
                          JOIN storage s ON s.id = e.storage_id 
            WHERE
                o.id = ? AND
                o.status = 1 AND 
                e.status = 1 AND
                s.mode IN ( ?, ? )
EOSQL

        my $rv = $sth->execute($object_id, STORAGE_MODE_READ_ONLY, STORAGE_MODE_READ_WRITE);

        my ($uri, $internal_name);
        $sth->bind_columns(\($uri, $internal_name));

        $entities = [];
        while ( $sth->fetchrow_arrayref ) {
            push @$entities, "$uri/$internal_name";
        }
        $sth->finish;

        my %h = map {
            ( $_ => Digest::MurmurHash::murmur_hash($_) )
        } @$entities;
        $entities = [ sort { $h{$a} <=> $h{$b} } keys %h ];

        if ( STF_DEBUG ) {
            print STDERR "[Get Entity] Backend entity Candidates:\n",
                map { "[Get Entity] + $_\n" } @$entities;
        }

        $self->cache_set( [ entities_for => $object_id ], $entities, $self->cache_expires );
    }

    my $object = $self->lookup( $object_id );

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
    my $repair = 0;

    # Send successive HEAD requests
    my $fastest;
    my $furl = $self->get('Furl');
    my $headers;
    if ( $if_modified_since ) {
        $headers = [ 'If-Modified-Since' => $if_modified_since ];
    }

    foreach my $entity ( @$entities ) {
        my (undef, $code) = $furl->head( $entity, $headers );
        if ( HTTP::Status::is_success( $code ) ) {
            if ( STF_DEBUG ) {
                print STDERR "[Get Entity] + HEAD $entity OK\n";
            }
            $fastest = $entity;
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
                print STDERR "[Get Entity] + HEAD $entity failed: $code\n";
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
        eval { $self->cache_delete( 'entities_for', $object_id ) };
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

1;
