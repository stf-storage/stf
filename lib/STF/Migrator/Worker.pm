package STF::Migrator::Worker;
use Mouse;
use Coro;
use Fcntl qw(SEEK_END LOCK_UN LOCK_EX LOCK_NB);
use FurlX::Coro;
use Guard;
use HTTP::Status;
use STF::Constants qw( STORAGE_MODE_TEMPORARILY_DOWN STORAGE_MODE_READ_WRITE STORAGE_MODE_READ_ONLY );

has concurrency => (
    is => 'ro',
    default => 10
);

has app => (
    is => 'ro',
    required => 1,
    handles => [ qw(conn) ],
);

has replicas => (
    is => 'ro',
    default => 3,
);

has storage_uri => (
    is => 'ro',
    required => 1,
);

has storage_id => (
    is => 'ro',
    required => 1,
);

has use_storage_as_source => (
    is => 'ro',
    default => 1,
);

has max_object_id => (
    is => 'ro',
);

has min_object_id => (
    is => 'ro',
);

has semaphore => (
    is => 'ro',
    lazy => 1,
    default => sub {
        my $self = shift;
        Coro::Semaphore->new($self->concurrency)
    },
    handles => {
        sem_guard => 'guard',
        sem_count => 'count',
    }
);

sub set_proc_name {
    my ($self, $message) = @_;

    my $fmt = "migrate-stf CHILD [%s] (%s -> %s)";
    if ( $message ) {
        $fmt .= " %s";
    }

    $0 = sprintf(
        $fmt,
        $self->storage_id,
        $self->max_object_id,
        $self->min_object_id,
        $message,
    );
}

sub run {
    my $self = shift;

    $self->set_proc_name();

    # Don't share my connections (DBIx::Connector handles this, but
    # I want to be EXPLICIT)
    $self->conn->disconnect;

    my $storage_id = $self->storage_id;
    my $max_object_id = $self->max_object_id;
    my $min_object_id = $self->min_object_id;
    my $loop = 1;
    my $selected = 0;
    my $replicated = 0;
    my $processed = 0;

    while ( $loop ) {
        my $object_ids = $self->conn->run(sub {
            $_->selectall_arrayref(<<EOSQL, undef, $storage_id, $max_object_id, $min_object_id );
                SELECT e.object_id
                    FROM entity e
                    FORCE INDEX (object_id)
                    WHERE e.storage_id = ? AND e.object_id <= ? AND e.object_id > ?
                    ORDER BY e.object_id DESC
EOSQL
        } );
        my $prev_id = $max_object_id;
        $selected += scalar @$object_ids;

        while ( @$object_ids ) {
            my $object_id = $max_object_id = (shift @$object_ids)->[0];
            $processed++;
            $self->set_proc_name("[$object_id, $replicated/$processed/$selected]");

            my $count = $self->get_current_replica_count( $object_id );
            if ($count >= $self->replicas) {
                printf "# %s already has %s replicas (%s)\n",
                    $object_id,
                    $count,
                    $$,
                ;
                next;
            }

            my $guard = $self->sem_guard();
            async_pool {
                my ($guard, $self, $object_id, $howmany) = @_;
                if ( $self->replicate( $object_id, $howmany ) ) {
                    $replicated++;
                }
            } $guard, $self, $object_id, $self->replicas - $count;
            cede;
        }

        if ( $max_object_id == 0 || $max_object_id == $prev_id ) {
            $loop = 0;
        }
    }

    while ( $self->sem_count < $self->concurrency ) {
        Coro::AnyEvent::sleep 1;
    }

    $self->set_proc_name("finished processing");
}

sub get_current_replica_count {
    my ($self, $object_id) = @_;
    my $count = $self->conn->run(sub {
        $_->selectrow_array( <<EOSQL, undef, $object_id, STORAGE_MODE_READ_ONLY, STORAGE_MODE_READ_WRITE, STORAGE_MODE_TEMPORARILY_DOWN );
            SELECT COUNT(*)
                FROM entity e JOIN storage s ON e.storage_id = s.id
                    WHERE e.object_id = ? AND s.mode IN ( ?, ?, ? )
EOSQL
    } );

    return $count;
}

sub replicate {
    my ($self, $object_id, $howmany) = @_;

    my $storage_id = $self->storage_id;
    my $success = guard {
        # lock lock lock
        my $filename = "storage-$storage_id.err";
        my $fh;
        if (! open $fh, '>>', $filename) {
            print STDERR "# Failed to open logfile $filename: $!\n";
            goto FAIL_LOCK;
        }

        my $locked;
        my $timeout = time() + 60;
        my $lock_guard = guard {
            if (Scalar::Util::openhandle($fh)) {
                eval { flock( $fh, LOCK_UN ) }
            }
        };

        while ( $timeout > time() ) {
            $locked = flock( $fh, LOCK_EX|LOCK_NB );
            if ($locked) {
                last;
            } else {
                usleep( int(rand( 1_000_000 )) );
            }
        }
        FAIL_LOCK:
        if (! $locked) {
            print STDERR "# Failed to lock log failure for $object_id\n";
        }
        if ($fh) {
            seek $fh, 0, SEEK_END;
            print $fh "$object_id\n";
            flock( $fh, LOCK_UN );
            close $fh;
            $lock_guard->cancel;
        }
    };

    my $conn = $self->conn;
    my $object = $conn->run(sub {
        $_->selectrow_hashref( <<EOSQL, undef, $object_id );
            SELECT o.id, o.internal_name
                FROM object o
                WHERE id = ? AND status = 1
EOSQL
    });
    if (! $object) {
        print "# no such object $object_id ($$)\n";
        return;
    }

    my $furl = FurlX::Coro::HTTP->new(timeout => 180);
    my $content =  $self->get_object( $furl, $object );
    if (! $content ) {
        print "# Could not get content for object $object->{id} ($$)\n";
        return;
    }

    my $storages = $conn->run(sub{
        $_->selectall_arrayref( <<EOSQL, { Slice => {} }, STORAGE_MODE_READ_WRITE, $object_id, STORAGE_MODE_READ_WRITE );
            SELECT s1.* FROM storage s1
                WHERE s1.mode = ? AND s1.id NOT IN (
                    SELECT s.id FROM storage s
                        JOIN entity e ON s.id = e.storage_id
                        WHERE e.object_id = ? AND s.mode = ?
                )
                ORDER BY rand()
EOSQL
    });

    my $copied = 0;
    foreach my $new_storage (@$storages) {
        $copied += $self->put_object( $furl, $object, $new_storage, $content );
        if ($copied >= $howmany) {
            last;
        }
    }

    if ($copied == $howmany) {
        print "# $object->{id} replicated $howmany times ($$)\n";
        $success->cancel;
        return 1;
    }

    return ();
}

sub get_object {
    my ($self, $furl, $object) = @_;

    # XXX If we're fetching from an already active storage, cool. just
    # use the same storage we're migrating from. Otherwise, get from
    # any existing entity, EXCEPT for the one we're migratin from

    my @uris;
    if ( $self->use_storage_as_source ) {
        my $uri = sprintf "%s/%s",
            $self->storage_uri,
            $object->{internal_name}
        ;
        push @uris, $uri;
    } else {
        my $conn = $self->conn;
        my $raw_uris = $conn->run(sub {
            $_->selectall_arrayref( <<EOSQL, undef, $object->{id} );
                SELECT WS_CONCAT("/", s.uri, o.internal_name) FROM 
                    entity e
                        JOIN object o ON e.object_id = o.id
                        JOIN storage s ON e.storage_id = s.id
                    WHERE o.id = ?
EOSQL
        });
        @uris = map { $_->[0] } @$raw_uris;
    }

    foreach my $uri ( @uris ) {
        my @res = $furl->get($uri);
        if ( ! HTTP::Status::is_success($res[1]) ) {
            print "# $object->{id}: GET $uri failed ($$)\n";
        } else {
            return $res[4];
        }
    }

    print "# $object->{id}: ALL get for object $object->{id} failed ($$)\n";
    return ();
}

sub put_object {
    my ($self, $furl, $object, $new_storage, $content) = @_;

    my $conn = $self->conn;
    my $new_uri = "$new_storage->{uri}/$object->{internal_name}";

    # avoid duplicates...
    my @get_res = $furl->head( $new_uri );

    # object already exists for some reason
    if (HTTP::Status::is_success( $get_res[1] )) {
        # does it already exist in entity table?
        my $t = $conn->run(sub {
            $_->selectrow_hashref( <<EOSQL, undef, $object->{id}, $new_storage->{id} );
                SELECT * FROM entity WHERE object_id = ? AND storage_id = ?
EOSQL
        });

        # if it already exists, then do nothing. probably a race condition
        # or some such. we tried.
        if ($t) {
            print "# $object->{id} exists at $new_uri. race condition? oh well...\n";
            return 1;
        }
        print "# $object->{id} exists at $new_uri, but entity doesn't exist. probably garbage? oh well... creating entity anyway\n";
        eval {
            $conn->run(sub {
                $_->do( <<EOSQL, undef, $new_storage->{id}, $object->{id}, time() );
                    INSERT INTO entity ( storage_id, object_id, created_at ) VALUES (?, ?, ?)
EOSQL
            });
            print "$object->{id}: object does not exist in entity for storage $new_storage->{id}. created ($$)\n";
        };
        if (my $e = $@) {
            if ( $e =~ /Duplicate/ ) {
                # Duplicate is ok. Probably a race condition
                return 1;
            }
            print "# creating entity for $object->{id} failed: $@ ($$)\n";
            return 0
        }
    } else {
        # if HEAD is a failure, then attempt to PUT
        my $put_tries = 1;
PUT_RETRY:
        my @put_res = $furl->put( $new_uri, undef, $content );
        if (! HTTP::Status::is_success( $put_res[1] ) ) {
            print "$object->{id}: Failed to put to $new_storage->{id} ($new_uri): $put_res[2] ($$)\n";
            if ( $put_tries++ > 5 ) {
                print "# $object->{id}: Failed to put $put_tries times ($new_uri)\n";
                return 0;
            }

            print "$object->{id}: Retry put $new_uri... ($$)\n";
            goto PUT_RETRY;
        }

        eval {
            $conn->run(sub {
                $_->do( <<EOSQL, undef, $new_storage->{id}, $object->{id}, time() );
                   INSERT INTO entity ( storage_id, object_id, created_at ) VALUES (?, ?, ?)
EOSQL
            });
            print "$object->{id}: copied to $new_storage->{id} ($$)\n";
        };
        if ($@) {
            print "# creating entity for $object->{id} failed: $@ ($$)\n";
            return 0;
        }
    }

    return 1;
}



no Mouse;

1;
