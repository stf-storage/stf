package STF::API::Object;
use strict;
use parent qw(STF::API::WithDBI);
use Digest::MurmurHash ();
use HTTP::Status ();
use STF::Constants qw(STF_DEBUG :object STORAGE_MODE_REMOVED);
use STF::Dispatcher::PSGI::HTTPException;
use Class::Accessor::Lite
    new => 1,
    rw => [ qw(furl urandom) ]
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
    my $dbh = $self->dbh('DB::Slave');
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

sub repair {
    my ($self, $object_id)= @_;

    my $object = $self->lookup( $object_id );
    if (! $object) {
        if (STF_DEBUG) {
            print STDERR "[    Repair] No matching object $object_id\n";
        }
        return;
    }

    my $entity_api = $self->get( 'API::Entity' );
    my @entities = $entity_api->search( { object_id => $object_id } );
    if (@entities) {
        if (STF_DEBUG) {
            printf STDERR "[    Repair] Loaded %d entities\n",
                scalar @entities
        }
    } else {
        if (STF_DEBUG) {
            print STDERR "[    Repair] No matching entities for $object_id ( XXX broken ? )\n";
        }
        return;
    }

    my $storage_api = $self->get('API::Storage');
    my $furl = $self->get('Furl');
    my $ref_url;
    my ($content, @intact, @broken);
    foreach my $entity ( @entities ) {
        my $storage = $storage_api->lookup( $entity->{storage_id} );
        if (! $storage) {
            if (STF_DEBUG) {
                print STDERR "[    Repair] storage $entity->{storage_id} does not exist\n";
            }
            push @broken, $entity->{storage_id};
            next;
        }

        # If the mode is REMOVED then we've purposely taken it out of the
        # system, and needs to be repaired. Also, if this were the case,
        # we DO NOT issue an DELETE on the backend, as it most likely will
        # not properly respond.
        if ($storage->{mode} == STORAGE_MODE_REMOVED) {
            print STDERR "[    Repair] Storage $storage->{id} has been removed. Adding to broken list.\n";
            push @broken, $storage->{id};
            next;
        }

        my $url = join "/", $storage->{uri}, $object->{internal_name};
        if (STF_DEBUG) {
            print STDERR "[    Repair] Going to check $url\n";
        }

        my (undef, $code) = eval { $furl->head( $url ) };
        if ($@) {
            print STDERR "[    Repair] HTTP request raised an exception: $@\n";
            # Make sure this becomes an error
            $code = 500;
        }

        my $is_success = HTTP::Status::is_success( $code );
        if (STF_DEBUG) {
            printf STDERR "[    Repair] HEAD %s was %s (%d)\n",
                $url, ($is_success ? "OK" : "FAIL"), $code;
        }

        if ($is_success) {
            $ref_url ||= $url;
            push @intact, $storage->{id};
        } else {
            push @broken, $storage->{id};
        }
    }

    if (! @intact) {
        printf STDERR "[    Repair] No entities available, object %s is COMPLETELY BROKEN\n", $object_id;
        $self->update( $object_id => { status => OBJECT_INACTIVE } );
        return;
    }

    if ( @broken ) {
        if (STF_DEBUG) {
            printf STDERR "[    Repair] Removing entities for %s in %s\n",
                $object_id,
                join ", ", @broken
            ;
        }
        $entity_api->delete( {
            storage_id => [ -in => @broken ],
            object_id => $object_id
        } );
    }

    my $need = $object->{num_replica};
    my $have = scalar @intact;
    if ($need <= $have) {
        if (STF_DEBUG) {
            printf STDERR "[    Repair] No need to repair %s (need %d, have %d)\n",
                $object_id,
                $need,
                $have,
            ;
        }

        return scalar @broken;
    } else {
        my $n = $need - $have;
        if (STF_DEBUG) {
            printf STDERR "[    Repair] Going to replicated %s %d times\n",
                $object_id,
                $n,
            ;
            print STDERR "[    Repair] Using content from $ref_url\n";
        }
        my (undef, undef, undef, undef, $content) = $furl->get( $ref_url );
        $entity_api->replicate( {
            object_id => $object_id,
            content   => $content,
            replicas  => $n,
        } );
        return $n + scalar @broken;
    }

}

sub get_any_valid_entity_url {
    my ($self, $args) = @_;

    my ($bucket_id, $object_name, $if_modified_since) =
        @$args{ qw(bucket_id object_name if_modified_since) };
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

    my $entities = $self->cache_get( 'entities_for', $object_id );
    if (! $entities) {
        my $dbh = $self->dbh('DB::Slave');
        my $sth = $dbh->prepare(<<EOSQL);
            SELECT s.uri, o.internal_name
            FROM object o JOIN entity e ON o.id = e.object_id
                          JOIN storage s ON s.id = e.storage_id 
            WHERE
                o.bucket_id = ? AND
                o.name = ? AND 
                o.status = 1 AND 
                e.status = 1 AND
                s.mode >= 0
EOSQL

        my $rv = $sth->execute($bucket_id, $object_name);

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

    # We need to put it in repair if entities < num_replica
    my $object = $self->lookup( $object_id );
    my $repair = @$entities < $object->{num_replica};

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
            printf STDERR "[Get Entity] Sending %s to repair\n",
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
