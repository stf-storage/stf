package STF::API::Entity;
use Mouse;
use Carp ();
use File::Temp ();
use Furl::HTTP;
use Scalar::Util ();
use STF::Constants qw(:entity :storage STF_DEBUG STF_TIMER STORAGE_MODE_READ_WRITE OBJECT_INACTIVE);
use STF::Log;
use STF::Utils ();

with 'STF::API::WithDBI';

has max_num_replica => (
    is => 'rw',
);

has min_num_replica => (
    is => 'rw'
);

sub search_with_url {
    my ($self, $where, $opts) = @_;

    my $s = $self->sql_maker->new_select;
    $s->add_select(\'entity.*');
    $s->add_select(
        \q{CONCAT_WS('/', storage.uri, object.internal_name)} => 'url'
    );
    $s->add_join( entity => {
        type => 'INNER',
        table => 'storage',
        condition => 'entity.storage_id = storage.id',
    }); 
    $s->add_join( entity => {
        type => 'INNER',
        table => 'object',
        condition => 'entity.object_id = object.id',
    });
    if ( $where ) {
        for my $key( keys %{$where} ) {
            $s->add_where( $key => $where->{$key} );
        }
    }
    $s->limit( $opts->{limit} ) if defined $opts->{limit};
    $s->offset( $opts->{offset} ) if defined $opts->{offset};
    $s->add_order_by( ref $opts->{order_by} eq 'HASH' ? %{ $opts->{order_by} } : $opts->{order_by} ) if defined $opts->{order_by};

    my $dbh = $self->dbh('DB::Master');
    my $result = $dbh->selectall_arrayref( $s->as_sql, { Slice => {} }, $s->bind );
    return wantarray ? @$result : $result;
}

sub delete_for_object_id {
    my ($self, $object_id) = @_;

    local $STF::Log::PREFIX = "Delete(O)";

    my $delobj_api = $self->get('API::DeletedObject');
    my $deleted = $delobj_api->lookup( $object_id );
    if ( ! $deleted ) {
        debugf("Object %s not found", $object_id) if STF_DEBUG;
        return ;
    }

    my $furl = $self->get('Furl');
    my $storage_api = $self->get('API::Storage');
    foreach my $entity ( $self->search( { object_id => $object_id } ) ) {
        my $storage_id = $entity->{storage_id};
        my $storage = $storage_api->lookup( $storage_id );
        if (! $storage) {
            debugf(
                "Could not find storage %s for object ID %s",
                $storage_id, $object_id
            ) if STF_DEBUG;
            next;
        }

        debugf(
            " + Deleting logical entity for %s on %s",
            $object_id, $storage->{id},
        ) if STF_DEBUG;

        $self->delete( {
            object_id => $object_id,
            storage_id => $storage->{id}
        } );

        # XXX REPAIR mode is done while the storage is online, so you need to
        # at least attempt to delete the object
        if ( $storage->{mode} != STORAGE_MODE_READ_WRITE && $storage->{mode} != STORAGE_MODE_REPAIR_NOW && $storage->{mode} != STORAGE_MODE_REPAIR ) {
            debugf(
                "Storage %s is known to be broken. Skipping physical delete request\n",
                $storage->{uri}
            ) if STF_DEBUG;
            next;
        }

        my $uri = join '/', $storage->{uri}, $deleted->{internal_name};
        debugf(" + Sending DELETE %s (object = %s)", $uri, $object_id) if STF_DEBUG;
        # Send requests to the backend.
        $furl->delete( $uri );
    }

    $delobj_api->delete( $object_id );
}

sub record {
    my ($self, $args) = @_;

    my $object_id = $args->{object_id} or die "XXX no object_id";
    my $storage_id = $args->{storage_id} or die "XXX no storage_id";

    # PUT was successful. Now write this to the database
    eval {
        my $store_sth = $self->get('DB::Master')->prepare( <<EOSQL );
            REPLACE
                INTO entity (object_id, storage_id, status, created_at)
                VALUES (?, ?, ?, UNIX_TIMESTAMP(NOW()))
EOSQL
        $store_sth->execute( $object_id, $storage_id, ENTITY_ACTIVE );
    };
    if ($@) {
        die "Failed to write new entity in database: $@";
    }
}

# Stores 1 entity for given object + storage. Also creates an entry in the 
# database. This code is will break if you use it in event-based code
sub store {
    my ($self, $args) = @_;
    local $STF::Log::PREFIX = "Store(E)" if STF_DEBUG;

    my $storage = $args->{storage} or die "XXX no storage";
    my $object  = $args->{object}  or die "XXX no object";
    my $content = $args->{content} or die "XXX no content";
    my $furl    = $self->get('Furl');

    my $storage_api = $self->get('API::Storage');
    if (! $storage_api->is_writable( $storage ) ) {
        if ( STF_DEBUG ) {
            debugf("Storage [%s] is not writable, skipping write", $storage->{id});
        }
        return;
    }

    my $uri = sprintf "%s/%s", $storage->{uri}, $object->{internal_name};

    # Handle the case where the destination already exists. If the
    # storage allows us to overwrite it then fine, but if it doesn't
    # we need to delete it
    if (STF_DEBUG) {
        debugf(
            "Sending DELETE %s (storage = %s, cluster = %s)",
            $uri, $storage->{id}, $storage->{cluster_id},
        );
    }
    eval {
        local $furl->{timeout} = 5;
        my (undef, $code) = $furl->delete($uri);
        if (STF_DEBUG) {
            my $ok = HTTP::Status::is_success($code);
            debugf(
                "        DELETE %s was %s (%s)",
                $uri,
                ($ok ? "OK" : "FAIL (harmless)"),
                $code
            );
        }
    };

    debugf(
        "Sending PUT %s (object = %s, storage = %s, cluster = %s)",
        $uri, $object->{id}, $storage->{id}, $storage->{cluster_id}
    ) if STF_DEBUG;

    if ( Scalar::Util::openhandle( $content ) ) {
        seek( $content, 0, 0 );
    }

    my $put_guard;
    if ( STF_TIMER ) {
        $put_guard = STF::Utils::timer_guard("STF::API::Entity::store [PUT]");
    }

    my @hdrs = (
        'Content-Length'         => $object->{size},
        'X-STF-Object-Timestamp' => $object->{created_at},
    );

    my (undef, $code, undef, $rhdrs, $body) = eval {
        $furl->put($uri, \@hdrs, $content);
    };
    if ($@) {
        $body = $@;
        $code = 500;
    }

    my $ok = HTTP::Status::is_success($code);
    debugf(
        "        PUT %s was %s (%s)",
        $uri,
        ($ok ? "OK" : "FAIL"),
        $code
    ) if STF_DEBUG;

    if ( !$ok ) {
        local $Log::Minimal::AUTODUMP = 1;
        critf("Request to store to %s failed:", $uri);
        critf("   code    = %s", $code);
        critf("   headers = ", $rhdrs);
        critf("===");
        critf($_) for split /\n/, $body;
        critf("===");
        return;
    }

    # PUT was successful. Now write this to the database
    $self->record({
        storage_id => $storage->{id},
        object_id  => $object->{id},
    });
    return 1;
} 

sub remove {
    my ($self, $args) = @_;

    local $STF::Log::PREFIX = "Remove(E)";
    my $object = $args->{object} or die "XXX no object";
    my $storages = $args->{storages} or die "XXX no storages";

    if (STF_DEBUG) {
        debugf( "Removing broken entities for %s in", $object->{id});
        foreach my $storage (@$storages) {
            debugf(" - [%d] %s", $storage->{id}, $storage->{uri} || '(null)');
        }
    }

    # Timeout fast!
    my $furl = $self->get('Furl');
    local $furl->{timeout} = 5;

    # Attempt to remove actual bad entities
    foreach my $broken ( @$storages ) {
        my $cache_key = [ "storage", $broken->{id}, "http_accessible" ];
        my $st        = $self->cache_get( @$cache_key );
        my $mode      = $broken->{mode};
        if ( ( defined $st && $st == -1 ) ||
             ( $mode != STORAGE_MODE_READ_WRITE &&
               $mode != STORAGE_MODE_REPAIR_NOW &&
               $mode != STORAGE_MODE_REPAIR )
        ) {
            debugf("Storage %s is known to be broken. Skipping delete request", $broken->{uri}) if STF_DEBUG;
            next;
        }

        my $url = join "/", $broken->{uri}, $object->{internal_name};
        debugf("Deleting entity %s for object %s", $url, $object->{id}) if STF_DEBUG;
        eval {
            my (undef, $code, $msg) = $furl->delete( $url );

            # XXX Remember which hosts would respond to HTTP
            # This is here to speed up the recovery process
            if ( $code eq 404 || HTTP::Status::is_success($code) ) {
                $self->delete( {
                    storage_id => $broken->{id},
                    object_id  => $object->{id},
                } );
            } elsif ( HTTP::Status::is_error($code) ) {
                # XXX This error code is probably not portable.
                if ( $msg =~ /(?:Cannot connect to|Failed to send HTTP request: Broken pipe)/ ) {
                    $self->cache_set( $cache_key, -1, 5 * 60 );
                }
            }
        };
        if ($@) {
            critf(
                "Error while deleting entity on storage %s for object %sj",
                $broken->{id}, $object->{id},
            );
        }
    }
}

sub check_health {
    my ($self, $args) = @_;

    local $STF::Log::PREFIX = "Health(E)";
    my $object_id = $args->{object_id} or die "XXX no object";
    my $storage_id = $args->{storage_id} or die "XXX no storage";

    my ($entity) = $self->get('API::Entity')->search({
        storage_id => $storage_id,
        object_id  => $object_id,
    });
    if (! $entity) {
        debugf(
            "Entity on storage %s for object %s is not recorded.",
            $object_id, $storage_id,
        ) if STF_DEBUG;
        return;
    }

    my $object = $self->get('API::Object')->lookup( $object_id );
    my $storage = $self->get('API::Storage')->lookup( $storage_id );

    # an entity in TEMPORARILY_DOWN node needs to be treated as alive
    if ($storage->{mode} == STORAGE_MODE_TEMPORARILY_DOWN) {
        debugf(
            "Storage %s is temporarily down. Assuming this is intact.",
            $storage->{id}
        ) if STF_DEBUG;
        return 1;
    }

    # If the mode is not in a readable state, then we've purposely 
    # taken it out of the system, and needs to be repaired. Also, 
    # if this were the case, we DO NOT issue an DELETE on the backend, 
    # as it most likely will not properly respond.
    my $storage_api = $self->get('API::Storage');
    if (! $storage_api->is_readable($storage)) {
        debugf(
            "Storage %s is not readable. Adding to invalid list.",
            $storage->{id}
        ) if STF_DEBUG;

        return ();
    }

    my $url = join "/", $storage->{uri}, $object->{internal_name};
    debugf(
        "Going to check %s (object_id = %s, storage_id = %s)",
        $url, $object_id, $storage_id,
    ) if STF_DEBUG;

    my $furl = $self->get('Furl');
    my $fh = File::Temp->new( UNLINK => 1 );
    my (undef, $code) = eval {
        $furl->request(
            url => $url,
            method => "GET",
            write_file => $fh,
        );
    };
    if ($@) {
        debugf("HTTP request raised an exception: %s", $@) if STF_DEBUG;
        # Make sure this becomes an error
        $code = 500;
    }

    my $is_success = HTTP::Status::is_success( $code );
    debugf("GET %s was %s (%d)", $url, ($is_success ? "OK" : "FAIL"), $code) if STF_DEBUG;

    $fh->flush;
    $fh->seek(0, 0);
    my $size = (stat($fh))[7];
    if ( $size != $object->{size} ) {
        $is_success = 0;
        debugf(
            "Object %s sizes do not match (got %d, expected %d)",
            $object->{id}, $size, $object->{size}
        ) if STF_DEBUG;
    }

    return $is_success;
}

sub fetch_content {
    my ( $self, $args ) = @_;

    local $STF::Log::PREFIX = "Fetch(E)" if STF_DEBUG;

    my $object = $args->{object} or die "XXX no object";
    my $storage = $args->{storage} or die "XXX no object";

    if (! $self->get('API::Storage')->is_readable($storage)) {
        debugf("Storage %s is not readable", $storage->{id}) if STF_DEBUG;
        return;
    }

    my $furl = $self->get('Furl');
    my $uri = join "/", $storage->{uri}, $object->{internal_name};
    debugf(
        "Sending GET %s (object = %s, storage = %s)",
        $uri,
        $object->{id},
        $storage->{id},
    ) if STF_DEBUG;

    my $fh = File::Temp->new( UNLINK => 1 );
    my (undef, $code, undef, $hdrs) = $furl->request(
        method     => 'GET',
        url        => $uri,
        write_file => $fh,
    );

    my $fetch_ok = $code eq '200';
    debugf(
        "        GET %s was %s (%s)",
        $uri, $fetch_ok ? "OK" : "FAIL", $code,
    ) if STF_DEBUG;
    
    if ( $code ne '200' ) {
        $fh->close;
        return;
    }

    $fh->flush;
    $fh->seek(0, 0);

    my $size = (stat($fh))[7];
    if ( $object->{size} != $size ) {
        debugf(
            "Fetched content size for object %s does not match registered size?! (got %d, expected %d)",
            $object->{id}, $size, $object->{size}
        ) if STF_DEBUG;
        $fh->close;
        return;
    }

    # success
    debugf(
        "Success fetching %s (object = %s, storage = %s)",
        $uri, $object->{id}, $storage->{id},
    ) if STF_DEBUG;
    return $fh;
}

sub fetch_content_from_any {
    my ($self, $args) = @_;

    my $object = $args->{object} or die "XXX no object";

    my $dbh = $self->dbh;
    my $storages = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, STORAGE_MODE_READ_ONLY, STORAGE_MODE_READ_WRITE, $object->{id} );
        SELECT s.id, s.uri 
            FROM storage s JOIN entity e ON s.id = e.storage_id
            WHERE s.mode IN (?, ?) AND e.object_id = ?
EOSQL

    if ( scalar( @$storages ) == 0) {
        debugf("No storage matching object %s found", $object->{id}) if STF_DEBUG;
        return;
    }

    my $content;
    foreach my $storage ( @$storages ) {
        # XXX We KNOW that these are readable.
        local $storage->{mode} = STORAGE_MODE_READ_ONLY;
        $content = $self->fetch_content( {
            object => $object,
            storage => $storage,
        } );
        last if defined $content;
    }

    return $content;
}

no Mouse;

1;
