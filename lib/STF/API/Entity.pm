package STF::API::Entity;
use Mouse;
use Carp ();
use Furl::HTTP;
use Scalar::Util ();
use STF::Constants qw(:entity :storage STF_DEBUG STF_TIMER STORAGE_MODE_READ_WRITE OBJECT_INACTIVE);
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

        if ( STF_DEBUG ) {
            printf STDERR "[DeleteObject] + Deleting logical entity for %s on %s\n",
                $object_id,
                $storage->{id},
            ;
        }
        $self->delete( {
            object_id => $object_id,
            storage_id => $storage->{id}
        } );

        my $uri = join '/', $storage->{uri}, $deleted->{internal_name};
        if ( STF_DEBUG ) {
            print STDERR "[DeleteObject] + Sending DELETE $uri\n";
        }

        if ( $storage->{mode} != STORAGE_MODE_READ_WRITE ) {
            if ( STF_DEBUG ) {
                printf STDERR "[DeleteObject] storage %s is known to be broken. Skipping delete request\n",
                    $storage->{uri}
                ;
            }
            next;
        }

        # Send requests to the backend.
        $furl->delete( $uri );
    }

    $delobj_api->delete( $object_id );
}

# Stores 1 entity for given object + storage. Also creates an entry in the 
# database. This code is will break if you use it in event-based code
sub store {
    my ($self, $args) = @_;

    my $storage = $args->{storage} or die "XXX no storage";
    my $object  = $args->{object}  or die "XXX no object";
    my $content = $args->{content} or die "XXX no content";
    my $furl    = $self->get('Furl');

    my $uri = sprintf "%s/%s", $storage->{uri}, $object->{internal_name};

    # Handle the case where the destination already exists. If the
    # storage allows us to overwrite it then fine, but if it doesn't
    # we need to delete it
    if ( STF_DEBUG ) {
        printf STDERR "[ Replicate] + Sending DELETE %s (storage = %s)\n",
            $uri, $storage->{id};
    }
    eval { $furl->delete($uri) };

    if ( STF_DEBUG ) {
        printf STDERR "[ Replicate] + Sending PUT %s (storage = %s)\n",
            $uri, $storage->{id};
    }

    if ( Scalar::Util::openhandle( $content ) ) {
        seek( $content, 0, 0 );
    }

    my $put_guard;
    if ( STF_TIMER ) {
        $put_guard = STF::Utils::timer_guard("STF::API::Entity::replicate [PUT]");
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
    if ( STF_DEBUG ) {
        printf STDERR "[ Replicate] PUT %s was %s\n", $uri, ($ok ? "OK" : "FAIL");
    }

    if ( !$ok ) {
        require Data::Dumper;
        print STDERR 
            "[ Replicate] Request to replicate to $uri failed:\n",
            "[ Replicate] code    = $code\n",
            "[ Replicate] headers = ", Data::Dumper::Dumper($rhdrs),
            "[ Replicate] ===\n$body\n===\n",
        ;

        return;
    }

    # PUT was successful. Now write this to the database
    eval {
        my $store_sth = $self->get('DB::Master')->prepare( <<EOSQL );
            INSERT
                INTO entity (object_id, storage_id, status, created_at)
                VALUES (?, ?, ?, UNIX_TIMESTAMP(NOW()))
EOSQL
        $store_sth->execute( $object->{id}, $storage->{id}, ENTITY_ACTIVE );
    };
    if ($@) {
        die "Failed to write new entity in database: $@";
    }
    return 1;
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
            $check_replica_timer = STF::Utils::timer_guard( "STF::API::Entity::replicate [check replicas]" );
        }
        # Only replicate if actual replicas (entities) < object->{num_replica}
        my ($count) = $dbh->selectrow_array( <<EOSQL, undef, $object_id, STORAGE_MODE_READ_WRITE );
            SELECT count(*)
                FROM entity e JOIN storage s ON e.storage_id = s.id
                WHERE object_id = ? AND s.mode = ?
EOSQL

        # short-circuit here if you only want N replicas max
        my $max_num_replica = $self->max_num_replica;
        if ( defined $max_num_replica && $max_num_replica <= $count ) {
            if ( STF_DEBUG ) {
                printf STDERR 
                    "[ Replicate] Object %s has %d entities, but max replicas for this system is %d (num_replica = %d)\n",
                    $object_id,
                    $count,
                    $max_num_replica,
                    $object->{num_replica}
                ;
            }
            return (); # no replication
        }

        my $num_replica = $object->{num_replica};
        my $min_num_replica = $self->min_num_replica;
        if (defined $min_num_replica && $num_replica < $min_num_replica) {
            $num_replica = $min_num_replica;
        }

        if ( $num_replica <= $count ) {
            if ( STF_DEBUG ) {
                printf STDERR
                    "[ Replicate] Object %s wants %d, but there are already %d entities\n",
                    $object_id, $num_replica, $count
                ;
            }
            return (); # no replication
        }
        $replicas = $num_replica - $count;
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



        # if filesize is more than 10MByte, use tempfile not in memory.
        if ( $object->{size} > 10_000_000 ) {
            my ($buf, $ret);
            my $size = $object->{size};
            my $sofar = 0;

            $content = File::Temp->new( UNLINK => 1, SUFFIX => '.uploadtemp' );
            if ( STF_DEBUG ) {
                printf STDERR
                    "[ Replicate] write content to temp_file( %s ) %d\n",
                    $content->filename, $object->{size} ;
            }
            if ( !$content ) {
                print STDERR "[ Replicate] fail open temp file.\n";
                return;
            }

            READCONTENTTEMP: while ( $sofar < $size ) {
                $ret = read( $input, $buf, $size );
                if ( not defined $ret ) {
                    Carp::croak("Failed to read from input: $!");
                } elsif ( $ret == 0 ) { # EOF
                    last READCONTENTTEMP;
                }
                print $content $buf;
                $sofar += $ret;
            }
            $content->flush;
            print STDERR "[ Replicate] write temp file. $sofar $size\n";
        }
        else {
            my ($buf, $ret);
            my $size = $object->{size};
            my $sofar = 0;
            $content = '';
            READCONTENT: while ( $sofar < $size ) {
                $ret = read( $input, $buf, $size );
                if ( not defined $ret ) {
                    Carp::croak("Failed to read from input: $!");
                } elsif ( $ret == 0 ) { # EOF
                    last READCONTENT;
                }
                $content .= $buf;
                $sofar += $ret;
            }
        }
    }

    if (! $content && $object ) {
        if ( STF_DEBUG ) {
            printf STDERR "[ Replicate] content is null, fetching content from existing entitites\n";
        }

        # Get at least 1 entry
        my $storages = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, STORAGE_MODE_READ_ONLY, STORAGE_MODE_READ_WRITE, $object_id );
            SELECT s.id, s.uri 
                FROM storage s JOIN entity e ON s.id = e.storage_id
                WHERE s.mode IN (?, ?) AND e.object_id = ?
EOSQL

        if ( scalar( @$storages ) == 0) {
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
                printf STDERR "[ Replicate] Fetching %s as replica source\n", $uri;
            }



            # if filesize is more than 10MByte, use tempfile not in memory.
            my $x_content = '';
            my %special_headers = ('content-length' => undef);
            my $res = $furl->request(
                url             => $uri,
                special_headers => \%special_headers,
                write_code      => sub {
                    my ( $status, $msg, $headers, $buf ) = @_;
                    if ($special_headers{'content-length'} > 10_000_000) {
                        if ( ref(\$x_content) eq 'SCALAR' ) {
                            $x_content = File::Temp->new( UNLINK => 1, SUFFIX => '.uploadtemp' );
                            if ( STF_DEBUG ) {
                                printf STDERR
                                    "[ Replicate] write content to temp_file( %s ) %d\n",
                                    $x_content->filename,
                                    $special_headers{'content-length'}
                                    ;
                            }
                        }
                        print $x_content $buf;
                    }
                    else {
                        $x_content .= $buf;
                    }
                }
            );

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

    my $storages = $dbh->selectall_arrayref(<<EOSQL, { Slice => {} }, STORAGE_MODE_READ_WRITE, $object_id);
        SELECT s.id, s.uri FROM storage s
            WHERE s.mode = ? AND s.id NOT IN (SELECT storage_id FROM entity WHERE object_id = ?)
        ORDER BY rand() LIMIT @{[ $replicas * 2 ]}
EOSQL
    if (@$storages < $replicas) {
        if ( STF_DEBUG ) {
            printf STDERR "[ Replicate] Wanted %d storages, but found %d\n", $replicas, scalar @$storages;
        }

        # short-circuit for worst case
        if ( @$storages < 1 ) {
            printf STDERR "[ Replicate] In fact, no storages were avilable. Bailing out of replicate()\n";
            return 0;
        }
    }

    if (STF_DEBUG) {
        printf STDERR "[ Replicate] Creating entities in the backend storages\n";
    }

    my $store_timer;
    if ( STF_TIMER ) {
        $store_timer = STF::Utils::timer_guard( "replicate [store all]" );
    }

    my $ok_count = 0;
    foreach my $storage ( @$storages ) {
        my $ok = $self->store( {
            object => $object,
            storage => $storage,
            content => $content,
        } );

        if ($ok) {
            $ok_count++;
        }

        last if $ok_count >= $replicas;
    }

    if ($ok_count <= 0) { 
        Carp::croak("*** ALL REQUESTS TO REPLICATE FAILED (wanted $replicas) ***");
    }

    if (STF_DEBUG) {
        print STDERR "[ Replicate] Replicated $ok_count times\n";
    }

    return $ok_count;
}

no Mouse;

1;
