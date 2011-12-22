package STF::Dispatcher;
use strict;
use feature 'state';
use parent qw( STF::Trait::WithDBI );
use File::Basename ();
use File::Temp ();
use Guard ();
use IPC::SysV qw(S_IRWXU S_IRUSR S_IWUSR IPC_CREAT IPC_NOWAIT SEM_UNDO);
use IPC::SharedMem;
use IPC::Semaphore;
use POSIX();
use STF::Constants qw(
    :entity
    :server
    HAVE_64BITINT
    STF_DEBUG
    STF_TIMER
    STF_NGINX_STYLE_REPROXY
    STF_NGINX_STYLE_REPROXY_ACCEL_REDIRECT_URL
);
use STF::Context;
use STF::Dispatcher::PSGI::HTTPException;
use Time::HiRes ();
use Class::Accessor::Lite
    rw => [ qw(
        cache
        cache_expires
        context
        connect_info
        host_id
        health_check_probability
        min_consistency
        mutex
        shared_mem
        shm_name
        sem_name
    ) ]
;

BEGIN {
    if (! HAVE_64BITINT) {
        if ( STF_DEBUG ) {
            print STDERR "[Dispatcher] You don't have 64bit int. Emulating using Math::BigInt\n";
        }
        require Math::BigInt;
        Math::BigInt->import;
    }
}

sub bootstrap {
    my $class = shift;
    my $context = STF::Context->bootstrap;
    my $config = $context->config;

    $class->new(
        cache_expires => 300,
        %{$config->{'Dispatcher'}},
        container => $context->container,
        context   => $context,
    );
}

sub _pack_head {
    my ($time, $serial) = @_;
    if ( HAVE_64BITINT ) {
        return pack( "ql", $time, $serial );
    } else {
        pack( "N2l", unpack( 'NN', $time ), $serial );
    }
}

sub _unpack_head {
    if ( HAVE_64BITINT ) {
        return unpack( "ql", shift() );
    } else {
        my $high = shift;
        my $low = shift;
        my $time = Math::BigInt->new(
            "0x" . unpack("H*", CORE::pack("N2", $high, $low)));
        my $serial = unpack( "l", shift() );
        return $time, $serial;
    }
}

sub new {
    my ($class, %args) = @_;

    my $self = bless {
        sem_name => File::Temp->new(UNLINK => 1),
        shm_name => File::Temp->new(UNLINK => 1),
        health_check_probability => 0.001,
        %args,
        parent   => $$,
    }, $class;

    if (! $self->{host_id} ) {
        Carp::croak("No host_id specified!");
    }

    my $semkey = IPC::SysV::ftok( $self->{sem_name}->filename );
    my $mutex  = IPC::Semaphore->new( $semkey, 1, S_IRUSR | S_IWUSR | IPC_CREAT );
    my $shmkey = IPC::SysV::ftok( $self->{shm_name}->filename );
    my $shm    = IPC::SharedMem->new( $shmkey, 24, S_IRWXU | IPC_CREAT );
    if (! $shm) {
        die "PANIC: Could not open shared memory: $!";
    }
    $mutex->setall(1);
    $shm->write( _pack_head( 0, 0 ), 0, 24 );

    $self->{mutex} = $mutex;
    $self->{shared_mem} = $shm;

    $self;
}

sub DESTROY {
    my $self = shift;
    if ( $self->{parent} == $$ ) {
        if ( STF_DEBUG ) {
            printf STDERR "[Dispatcher] Cleaning up semaphore and shared memroy\n";
        }
        local $@;
        eval { $self->{mutex}->remove };
        eval { $self->{shared_mem}->remove };
    }
}

sub start_request {
    my ($self, $env) = @_;
    if ( STF_DEBUG ) {
        printf STDERR "[Dispatcher] Starting request scope\n";
    }
    return $self->container->new_scope();
}

sub handle_exception {
    my ($self, $e) = @_;

    if (ref $e eq 'ARRAY') {
        STF::Dispatcher::PSGI::HTTPException->throw(@$e);
    }
}

sub get_bucket {
    my ($self, $args) = @_;

    my ($bucket) = $self->get('API::Bucket')->lookup_by_name( $args->{bucket_name} );
    if ( STF_DEBUG ) {
        if ( $bucket ) {
            print STDERR "[Dispatcher] Found bucket $args->{bucket_name}:\n";
        } else {
            printf STDERR "[Dispatcher] Could not find bucket $args->{bucket_name}\n";
        }
    }
    return $bucket || ();
}

sub create_id {
    my ($self)  = @_;

    my $mutex = $self->mutex;
    my $shm   = $self->shared_mem;

    if ( STF_DEBUG > 1 ) {
        printf STDERR "[Dispatcher] $$ ATTEMPT to LOCK\n";
    }

    my ($rc, $errno);
    my $acquire = 0;
    do {
        $acquire++;
        $rc = $mutex->op( 0, -1, SEM_UNDO | IPC_NOWAIT );
        $errno = $!;
        if ( $rc <= 0 ) {
            Time::HiRes::usleep( int( rand(5_000) ) );
        }
    } while ( $rc <= 0 && $acquire < 100);

    if ( $rc <= 0 ) {
        die sprintf
            "[Dispatcher] SEMAPHORE: Process $$ failed to acquire mutex (tried %d times, \$! = %d, rc = %d, val = %d, zcnt = %d, ncnt = %d, id = %d)\n",
            $acquire,
            $errno,
            $rc,
            $mutex->getval(0),
            $mutex->getzcnt(0),
            $mutex->getncnt(0),
            $mutex->id
        ;
    }

    if ( STF_DEBUG > 1 ) {
        printf STDERR "[Dispatcher] $$ SUCCESS LOCK mutex\n"
    }

    Guard::scope_guard {
        if ( STF_DEBUG > 1 ) {
            printf STDERR "[Dispatcher] $$ UNLOCK mutex\n"
        }
        $mutex->op( 0, 1, SEM_UNDO );
    };

    my $host_id = (int($self->host_id + $$)) & 0xffff; # 16 bits
    my $time_id = time();

    my ($shm_time, $shm_serial) = _unpack_head( $shm->read(0, 24) );
    if ( $shm_time == $time_id ) {
        $shm_serial++;
    } else {
        $shm_serial = 1;
    }

    if ( $shm_serial >= (1 << SERIAL_BITS) - 1) {
        # Overflow :/ we received more than SERIAL_BITS
        die "serial bits overflowed";
    }
    $shm->write( _pack_head( $time_id, $shm_serial ), 0, 24 );

    my $time_bits = ($time_id - EPOCH_OFFSET) << TIME_SHIFT;
    my $serial_bits = $shm_serial << SERIAL_SHIFT;
    my $id = $time_bits | $serial_bits | $host_id;

    return $id;
}

sub create_bucket {
    my ($self, $args) = @_;

    state $txn = sub {
        my ($self, $id, $bucket_name) = @_;
        my $bucket_api = $self->get('API::Bucket');
        $bucket_api->create({
            id   => $id,
            name => $bucket_name,
        } );
    };

    my $res = $self->txn_block( 'DB::Master' => $txn, $self->create_id, $args->{bucket_name} );
    if (my $e = $@) {
        if (STF_DEBUG) {
            printf STDERR "Failed to create bucket: $e\n";
        }
        $self->handle_exception($e);
    }

    return $res || ();
}

sub delete_bucket {
    my ($self, $args) = @_;

    state $txn = sub {
        my ($self, $id ) = @_;
        my $bucket_api = $self->get('API::Bucket');

        # Puts the bucket into deleted_bucket
        my $rv = $bucket_api->mark_for_delete( { id => $id } );
        if ($rv == 0) {
            STF::Dispatcher::PSGI::HTTPException->throw( 403, [], [] );
        }

        return 1;
    };

    my ($bucket, $recursive) = @$args{ qw(bucket) };
    my $res = $self->txn_block( 'DB::Master' => $txn, $bucket->{id} );
    if (my $e = $@) {
        if (STF_DEBUG) {
            print STDERR "[Dispatcher] Failed to delete bucket: $e\n";
        }
        $self->handle_exception($e);
    } else {
        if ( STF_DEBUG ) {
            printf STDERR "[Dispatcher] Deleted bucket %s (%s)\n",
                $bucket->{name},
                $bucket->{id}
            ;
        }
    }

    if ($res) {
        # Worker does the object deletion
        $self->enqueue( delete_bucket => $bucket->{id} );
    }

    return $res || ();
}

sub is_valid_object {
    my ($self, $args) = @_;
    my ($bucket, $object_name ) = @$args{ qw( bucket object_name ) };

    my $object_api = $self->get( 'API::Object' );
    return $object_api->find_active_object_id( {
        bucket_id => $bucket->{id},
        object_name => $object_name
    } );
}

sub create_object {
    my ($self, $args) = @_;

    my $timer;
    if ( STF_TIMER ) {
        $timer = STF::Utils::timer_guard();
    }

    state $txn = sub {
        my $txn_timer;
        if ( STF_TIMER ) {
            $txn_timer = STF::Utils::timer_guard( "create_object (txn closure)");
        }

        my ($self, $object_id, $bucket_id, $replicas, $object_name, $size, $consistency, $suffix, $input) = @_;

        my $object_api = $self->get( 'API::Object' );
        my $entity_api = $self->get( 'API::Entity' );

        # check if this object has already been created before.
        # if it has, make sure to delete it first
        if ( STF_DEBUG ) {
            printf STDERR "[Dispatcher] Create object checking if object '%s' on bucket '%s' exists\n",
                $bucket_id,
                $object_name,
            ;
        }

        my $find_object_timer;
        if ( STF_TIMER ) {
            $find_object_timer = STF::Utils::timer_guard( "create_object (find object)");
        }
        my $old_object_id = $object_api->find_object_id( {
            bucket_id =>  $bucket_id,
            object_name => $object_name
        } );
        undef $find_object_timer;

        if ( $old_object_id ) {
            if ( STF_DEBUG ) {
                printf STDERR "[Dispatcher] Object '%s' on bucket '%s' already exists\n",
                    $object_name,
                    $bucket_id,
                ;
            }
            $object_api->mark_for_delete( $old_object_id );
        }

        my $insert_object_timer;
        if ( STF_TIMER ) {
            $insert_object_timer = STF::Utils::timer_guard( "create_object (insert)" );
        }
        my $internal_name = $object_api->create_internal_name( { suffix => $suffix } );
        # Create an object entry. This is the "master" reference to the object.
        $object_api->create({
            id            => $object_id,
            bucket_id     => $bucket_id,
            object_name   => $object_name,
            internal_name => $internal_name,
            size          => $size,
            replicas      => $replicas,
        } );

        undef $insert_object_timer;

        # Create entities. These are the actual entities which are replicated
        # across the system
        # XXX - We're calling "replicate" here, but this here is for "consistency"

        my $min = $self->min_consistency;
        if ( defined $min && $consistency < $min ) {
            if ( STF_DEBUG ) {
                printf STDERR "[Dispatcher] Got consistency %d, but our minimum consistency is %d\n",
                    $consistency, $min
            }
            $consistency = $min;
        }
        my $replicated = $entity_api->replicate({
            object_id => $object_id,
            replicas  => $consistency,
            input     => $input,
        });

        return (1, $old_object_id);
    };

    my ($bucket, $replicas, $object_name, $size, $consistency, $suffix, $input) = 
        @$args{ qw( bucket replicas object_name size consistency suffix input) };

    if ( STF_DEBUG ) {
        printf STDERR "[Dispatcher] Create object %s/%s\n",
            $bucket->{name},
            $object_name
        ;
    }
    my $object_id = $self->create_id();

    my $txn_block_timer;
    if (STF_TIMER) {
        $txn_block_timer = STF::Utils::timer_guard( "create_object (txn_block)" );
    }

    my ($res, $old_object_id) = $self->txn_block( 'DB::Master' => $txn,
        $object_id, $bucket->{id}, $replicas, $object_name, $size, $consistency, $suffix, $input );
    if (my $e = $@) {
        if (STF_DEBUG) {
            print STDERR "Error while creating object: $e\n";
        }
        $self->handle_exception($e);
        return ();
    }
    undef $txn_block_timer;

    my $post_timer;
    if (STF_TIMER) {
        $post_timer = STF::Utils::timer_guard( "create_object (post process)" );
    }

    if ($old_object_id) {
        if ( STF_DEBUG ) {
            print STDERR
                "[Dispatcher] Request $bucket->{name}/$object_name was for existing content.\n",
                " + Will queue request to delete old object ($old_object_id)\n"
            ;
        }
        $self->enqueue( delete_object => $old_object_id );
    }

    $self->enqueue( replicate => $object_id );
    undef $post_timer;

    return $res;
}

sub get_object {
    my ($self, $args) = @_;

    my $timer;
    if ( STF_TIMER ) {
        $timer = STF::Utils::timer_guard();
    }

    my ($bucket, $object_name, $force_master, $req) =
        @$args{ qw(bucket object_name force_master request) };

    my $if_modified_since = $req->header('If-Modified-Since');
    my $object_api = $self->get('API::Object');
    my $uri = $object_api->get_any_valid_entity_url({
        bucket_id         => $bucket->{id},
        object_name       => $object_name,
        if_modified_since => $if_modified_since,

        # XXX forcefully check the health of this object randomly
        health_check      => rand() < $self->health_check_probability,
    });
    if ($uri) {
        if ( STF_NGINX_STYLE_REPROXY ) {
            # nginx emulation of X-Reproxy-URL
            # location /reproxy {
            #     internal;
            #     set $reproxy $upstream_http_x_reproxy_url;
            #     proxy_pass $reproxy;
            #     proxy_hide_header Content-Type;
            # }
            STF::Dispatcher::PSGI::HTTPException->throw(
                200,
                [
                    'X-Accel-Redirect' => STF_NGINX_STYLE_REPROXY_ACCEL_REDIRECT_URL,
                    'X-Reproxy-URL' => $uri,
                ],
            );
        } else {
            STF::Dispatcher::PSGI::HTTPException->throw(
                200,
                [
                    'X-Reproxy-URL' => $uri,
                ],
            );
        }
        STF::Dispatcher::PSGI::HTTPException->throw( 200, [ 'X-Reproxy-URL' => $uri ], [] );
    }

    if ( STF_DEBUG ) {
        print STDERR "[Dispatcher] get_object() could not find suitable entity for $object_name\n";
    }

    return ();
}

sub delete_object {
    my ($self, $args) = @_;

    my $timer;
    if ( STF_TIMER ) {
        $timer = STF::Utils::timer_guard();
    }

    state $txn = sub {
        my ($self, $bucket_id, $object_name) = @_;
        my $object_api = $self->get( 'API::Object' );
        my $object_id = $object_api->find_object_id( {
            bucket_id => $bucket_id,
            object_name => $object_name
        } );

        if (! $object_id) {
            if ( STF_DEBUG ) {
                printf STDERR "[Dispatcher] No matching object_id found for DELETE\n";
            }
            return ();
        }

        if (! $object_api->mark_for_delete( $object_id ) ) {
            STF::Dispatcher::PSGI::HTTPException->throw( 500, [], [] );
        }

        return (1, $object_id);
    };

    my ($bucket, $object_name) = @$args{ qw(bucket object_name) };
    my ($res, $object_id) = $self->txn_block( 'DB::Master' => $txn, $bucket->{id}, $object_name);
    if (my $e = $@) {
        if (STF_DEBUG) {
            print STDERR "Failed to delete object: $e\n";
        }
        $self->handle_exception($e);
        return ();
    }

    if ($object_id) {
        $self->enqueue( delete_object => $object_id );
    }
    return $res || ();
}

sub modify_object {
    my ($self, $args) = @_;

    my ($bucket, $object_name, $replicas) = @$args{ qw(bucket object_name replicas) };
    if ( STF_DEBUG ) {
        printf STDERR "[Dispatcher] Modifying %s/%s (replicas = %d)\n",
            $bucket->{name},
            $object_name,
            $replicas
        ;
    }

    state $txn = sub {
        my ($self, $bucket_id, $object_name, $replicas) = @_;
        my $object_api = $self->get('API::Object');
        my $object_id = $object_api->find_active_object_id( {
            bucket_id => $bucket_id,
            object_name =>  $object_name
        } );
        if (! $object_id) {
            printf STDERR "[Dispatcher] Create object checking if object '%s' on bucket '%s' exists\n",
                $bucket_id,
                $object_name,
            ;
            return ();
        }

        $object_api->update($object_id => {
            num_replica => $replicas
        });
        if ( STF_DEBUG ) {
            printf STDERR "[Dispatcher] Updated %s to num_replica = %d\n",
                $object_id,
                $replicas
            ;
        }

        return $object_id;
    };

    my ($object_id) = $self->txn_block( 'DB::Master' => $txn, $bucket->{id}, $object_name, $replicas);
    if (my $e = $@) {
        if (STF_DEBUG) {
            printf STDERR "[Dispatcher]: Failed to modify object: %s\n", $e;
        }
        $self->handle_exception($e);
        return ();
    }

    if ( $object_id ) {
        $self->enqueue( replicate => $object_id );
    }

    return (!(!$object_id)) || ();
}

sub enqueue {
    my ($self, $func, $object_id) = @_;

    my $queue_api = $self->get( 'API::Queue' );
    my $rv = eval { $queue_api->enqueue( $func, $object_id ) };
    if ($@) {
        # XXX This should not be seen by the client,
        # but we need to make sure to log it
        printf STDERR "[Dispatcher] Error while enqueuing: %s\n + func: %s\n + object ID = %s\n",
            $@,
            $func,
            $object_id,
        ;
    }
    return $rv;
}

1;

__END__

=head1 NAME

STF::Dispatcher - Dispatcher For STF

=head1 SYNOPSIS

    use STF::Dispatcher::PSGI;
    use STF::Dispatcher;

    my $impl = STF::Dispatcher->new(
        host_id => 100, # number
        ...
    );

    STF::Dispatcher::PSGI->new(impl => $impl)->to_app;

=cut
