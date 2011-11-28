package STF::API::Entity;
use strict;
use Carp ();
use Furl::HTTP;
use Scalar::Util ();
use STF::Constants qw(:entity :storage STF_DEBUG STF_TIMER OBJECT_INACTIVE);
use STF::Utils ();
use parent qw( STF::API::WithDBI );
use Class::Accessor::Lite
    new => 1,
;

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

    my $dbh = $self->dbh('DB::Slave');
    my $result = $dbh->selectall_arrayref( $s->as_sql, { Slice => {} }, $s->bind );
    return wantarray ? @$result : $result;
}

sub delete_for_object_id {
    my ($self, $object_id) = @_;

    my $delobj_api = $self->get('API::DeletedObject');
    my $deleted = $delobj_api->lookup( $object_id );
    if ( ! $deleted ) {
        if (STF_DEBUG) {
            print STDERR "Object $object_id not found\n";
        }
        return ;
    }

    my $furl = $self->get('Furl');
    my $storage_api = $self->get('API::Storage');
    my @coros;
    foreach my $entity ( $self->search( { object_id => $object_id } ) ) {
        my $storage_id = $entity->{storage_id};
        my $storage = $storage_api->lookup( $storage_id );
        if (! $storage) {
            if ( STF_DEBUG ) {
                print STDERR "[DeleteObject] Could not find storage %s for object ID %s\n",
                    $storage_id,
                    $object_id
                ;
            }
            next;
        }

        my $uri = join '/', $storage->{uri}, $deleted->{internal_name};

        if ( STF_DEBUG ) {
            print STDERR " + Sending DELETE $uri\n";
        }
        # Send requests to the backend.
        my (undef, $code) = $furl->delete( $uri );
        if ($code ne '204') {
            $self->update(
                { storage_id => $storage->{id}, object_id => $object_id },
                { status => ENTITY_INACTIVE },
            );
        } else {
            $self->delete( {
                object_id => $object_id,
                storage_id => $storage->{id}
            } );
        }
    }

    $delobj_api->delete( $object_id );
}

sub replicate {
    my ($self, $args) = @_;

    my $replicate_guard;
    if ( STF_TIMER ) {
        $replicate_guard = STF::Utils::timer_guard("STF::API::Entity::replicate [ALL]");
    }

    my ($object_id, $replicas, $content, $input) =
        @$args{ qw(object_id replicas content input) };
    if ( STF_DEBUG ) {
        printf STDERR 
            "[ Replicate] Replicating object ID %s\n",
            $object_id;
    }
    my $dbh = $self->dbh;

    my $object = $self->get('API::Object')->lookup( $object_id );
    if (! $object) {
        if ( STF_DEBUG ) {
            printf STDERR 
                "[ Replicate] object %s does not exist\n",
                $object_id;
        }
        return ();
    }

    if (! defined $replicas ) {
        my $check_replica_timer;
        if ( STF_TIMER ) {
            $check_replica_timer = STF::Utils::timer_guard( "replicate [check replicas]" );
        }
        # Only replicate if actual replicas (entities) < object->{num_replica}
        my ($count) = $dbh->selectrow_array( <<EOSQL, undef, $object_id, STORAGE_MODE_READ_WRITE );
            SELECT count(*)
                FROM entity e JOIN storage s ON e.storage_id = s.id
                WHERE object_id = ? AND s.mode = ?
EOSQL
        if ( $object->{num_replica} <= $count ) {
            if ( STF_DEBUG ) {
                printf STDERR
                    "[ Replicate] Object %s wants %d, but there are already %d entities\n",
                    $object_id, $object->{num_replica}, $count
                ;
            }
            return (); # no replication
        }
        $replicas = $object->{num_replica} - $count;
    }
        
    # Select the storages. 
    if (!defined $replicas || $replicas <= 0) {
        $replicas = 1;
    }
    if ( STF_DEBUG ) {
        print STDERR "[ Replicate] Object $object_id will be replicated $replicas times\n";
    }

    my $furl = $self->get('Furl');

    if ( ! $content && $input ) {
        if ( STF_DEBUG ) {
            printf STDERR "[ Replicate] content is null, reading from input\n";
        }

        my $read_timer;
        if ( STF_TIMER ) {
            $read_timer = STF::Utils::timer_guard( "replicate [read content]" );
        }

        # This needs to be saved into a temporary file, and then each
        # coro will reopen it so it can pass a file handle
        my $tempinput = File::Temp->new( CLEANUP => 1 );
        my ($buf, $ret);
        READCONTENT: while ( 1 ) {
            $ret = read( $input, $buf, 10 * 1024 );
            if ( not defined $ret ) {
                Carp::croak("Failed to read from input: $!");
            } elsif ( $ret == 0 ) { # EOF
                last READCONTENT;
            }
            # use syswrite?
            print $tempinput $buf;
        }
        $tempinput->flush;
        $tempinput->seek(0, 0);
        $content = $tempinput;
    }

    if (! $content && $object ) {
        if ( STF_DEBUG ) {
            printf STDERR "[ Replicate] content is null, fetching content from existing entitites\n";
        }

        # Get at least 1 entry
        my $storages = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, $object_id );
            SELECT s.id, s.uri 
                FROM storage s JOIN entity e ON s.id = e.storage_id
                WHERE s.mode = 1 AND e.object_id = ?
EOSQL

        my $count = scalar @$storages;
        if ( STF_DEBUG ) {
            printf STDERR "[ Replicate] Found %d storages\n", $count;
        }

        if ($count == 0) {
            if ( STF_DEBUG ) {
                printf STDERR "[ Replicate] No storage matching object found, disableing object '%s'\n", $object_id;
            }
            $self->get('API::Object')->update( $object_id => { status => OBJECT_INACTIVE } );
            return;
        }


        my $furl = $self->get('Furl');
        foreach my $storage ( @$storages ) {
            my $uri = join "/", $storage->{uri}, $object->{internal_name};
            if ( STF_DEBUG ) {
                printf STDERR "[ Replicate] Fetching %s\n", $uri;
            }
            my (undef, $code, undef, $hdrs, $x_content) = $furl->get( $uri );
            if ( $code ne '200' ) {
                next;
            }

            # success
            if ( STF_DEBUG ) {
                printf STDERR "[ Replicate] Success, going to use content from %s\n", $uri;
            }
            $content = $x_content;
            last;
        }

        if ( ! $content ) {
            if ( STF_DEBUG ) {
                printf STDERR "[ Replicate] Failed to get content for %s\n",
                    $object_id
                ;
            }
            return;
        }
    }

    my $storages = $dbh->selectall_arrayref(<<EOSQL, { Slice => {} }, $object_id);
        SELECT s.id, s.uri FROM storage s
            WHERE s.mode = 1 AND s.id NOT IN (SELECT storage_id FROM entity WHERE object_id = ?)
        ORDER BY rand() LIMIT $replicas
EOSQL
    if (@$storages < 1) {
        Carp::croak("No storage found");
    }

    my @hdrs = (
        'Content-Length' => $object->{size},
        'X-STF-Object-Timestamp' => $object->{created_at},
#        'Content-Type'   => $req->content_type || 'text/plain',
    );
    my @coros;

    if (STF_DEBUG) {
        printf STDERR "[ Replicate] Creating entities in the backend storages\n";
    }

    my $store_timer;
    if ( STF_TIMER ) {
        $store_timer = STF::Utils::timer_guard( "replicate [store all]" );
    }

    my $store_sth = $dbh->prepare( <<EOSQL );
        INSERT
            INTO entity (object_id, storage_id, status, created_at)
            VALUES (?, ?, ?, UNIX_TIMESTAMP(NOW()))
EOSQL
    my $ok_count = 0;
    foreach my $storage ( @$storages ) {
        my $uri = sprintf "%s/%s", $storage->{uri}, $object->{internal_name};
        if ( STF_DEBUG ) {
            printf STDERR "[ Replicate] + Sending PUT %s (storage = %s)\n",
                $uri, $storage->{id};
        }

        my $req_content = $content;
        if ( Scalar::Util::openhandle( $content ) && $content->can('filename') ) {
            open my $xfh, '<', $content->filename
                or die sprintf "Failed to open %s", $content->filename;
            $req_content = $xfh;
        }

        my $put_guard;
        if ( STF_TIMER ) {
            $put_guard = STF::Utils::timer_guard("STF::API::Entity::replicate [PUT]");
        }

        my (undef, $code, undef, $rhdrs, $body) = eval {
            $furl->put($uri, \@hdrs, $req_content);
        };
        if ($@) {
            $body = $@;
            $code = 500;
        }

        my $ok = HTTP::Status::is_success($code);
        if ( $ok ) {
            $ok_count++;
            # PUT was successful. Now write this to the database
            $store_sth->execute( $object_id, $storage->{id}, ENTITY_ACTIVE );
        } else {
            require Data::Dumper;
            print STDERR 
                "[ Replicate] Request to replicate to $uri failed:\n",
                "[ Replicate] code    = $code\n",
                "[ Replicate] headers = ", Data::Dumper::Dumper($rhdrs),
                "[ Replicate] ===\n$body\n===\n",
            ;
        }

        if ( STF_DEBUG ) {
            printf STDERR "[ Replicate] PUT %s was %s\n", $uri, ($ok ? "OK" : "FAIL");
        }
    }

    if ($ok_count <= 0) { 
        Carp::croak("*** ALL REQUESTS TO REPLICATE FAILED (wanted $replicas) ***");
    }

    return $ok_count;
}

1;
