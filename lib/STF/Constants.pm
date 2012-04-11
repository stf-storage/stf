package STF::Constants;
use strict;
use parent qw(Exporter);
use Config ();

my %constants;
BEGIN {
    %constants = (
        STF_CACHE_DEBUG => $ENV{STF_CACHE_DEBUG} || 0,
        STF_DEBUG => $ENV{STF_DEBUG} || 0,
        STF_TIMER => $ENV{STF_TIMER} || 0,
        STF_NGINX_STYLE_REPROXY => $ENV{STF_NGINX_STYLE_REPROXY},
        STF_NGINX_STYLE_REPROXY_ACCEL_REDIRECT_URL => $ENV{ STF_NGINX_STYLE_REPROXY_ACCEL_REDIRECT_URL } ||= "/reproxy",

        STF_ENABLE_STORAGE_META => $ENV{ STF_ENABLE_STORAGE_META },
        STF_ENABLE_OBJECT_META => $ENV{ STF_ENABLE_OBJECT_META },

        OBJECT_ACTIVE => 1,
        OBJECT_INACTIVE => 0,
        ENTITY_ACTIVE => 1,
        ENTITY_INACTIVE => 0,

        HAVE_64BITINT => $Config::Config{use64bitint},

        EPOCH_OFFSET  => 946684800,
        HOST_ID_BITS  => 16,
        TIME_BITS     => 36,
        Q4M_FUNC_REPLICATE     => 1,
        Q4M_FUNC_DELETE_OBJECT => 2,
        Q4M_FUNC_DELETE_BUCKET => 3,
        Q4M_FUNC_REPAIR_OBJECT => 4,
        Q4M_FUNC_OBJECT_HEALTH => 5,

        STORAGE_CLUSTER_MODE_READ_ONLY  => 0,
        STORAGE_CLUSTER_MODE_READ_WRITE => 1,

        STORAGE_MODE_CRASH_RECOVERED => -4,
        STORAGE_MODE_CRASH_RECOVER_NOW => -3,
        STORAGE_MODE_CRASH => -2,
        # These are currently in need of reconsideration.

        STORAGE_MODE_TEMPORARILY_DOWN => -1,
        # Denotes that the storage is temporarily down. Use it to stop
        # the dispatcher from accessing a storage for a short period
        # of time while you do minor maintenance work. No GET/PUT/DELETE
        # will be issued against this node while in this mode.
        #
        # Upon repair worker hitting this node, the entity is deemed
        # alive, and no new entities are created to replace it.
        # This is why you should only use this mode TEMPORARILY.

        STORAGE_MODE_READ_ONLY => 0,
        # Denotes that the storage is for read-only. No PUT/DELETE operations
        # will be issued against this node while in this mode.
        #
        # An entity residing on this node is deemed alive.

        STORAGE_MODE_READ_WRITE => 1,
        # Denotes that the storage is for read-write. 
        # This is the default, and the most "normal" mode for a storage.

        STORAGE_MODE_RETIRE => 2,
        # Denotes that the storage has been retired. Marking a storage as
        # retired means that the storage is not to be put back again.
        #
        # Entities residing on this node are deemed dead. Upon repair,
        # the worker(s) will try to replace the missing entity with
        # a new copy from some other node.

        STORAGE_MODE_MIGRATE_NOW => 3,
        STORAGE_MODE_MIGRATED => 4,
        # These are only used to denote that an automatic migration
        # is happening

        STORAGE_MODE_SPARE => 10,
        # Denotes that the storage is a spare for the registered cluster.
        # Writes are performed, but reads do not happen. Upon a failure
        # you can either replace the broken storage with this one, or
        # use this to restore the broken storage.
    );
    $constants{ SERIAL_BITS  } = (64 - $constants{HOST_ID_BITS} - $constants{TIME_BITS});
    $constants{ TIME_SHIFT   } = $constants{HOST_ID_BITS} + $constants{SERIAL_BITS};
    $constants{ SERIAL_SHIFT } = $constants{HOST_ID_BITS};
}

use constant \%constants;
sub as_hashref { \%constants }

my @object = qw(OBJECT_INACTIVE OBJECT_ACTIVE);
my @entity = qw(ENTITY_INACTIVE ENTITY_ACTIVE);
my @storage = qw(
    STORAGE_CLUSTER_MODE_READ_ONLY
    STORAGE_CLUSTER_MODE_READ_WRITE
    STORAGE_MODE_REMOVED
    STORAGE_MODE_CRASH_RECOVERED
    STORAGE_MODE_CRASH_RECOVER_NOW
    STORAGE_MODE_CRASH                    
    STORAGE_MODE_TEMPORARILY_DOWN
    STORAGE_MODE_READ_ONLY
    STORAGE_MODE_READ_WRITE
    STORAGE_MODE_RETIRE
    STORAGE_MODE_MIGRATE_NOW
    STORAGE_MODE_MIGRATED
);
my @func = grep { /^Q4M_FUNC/ } keys %constants;
my @server = qw(EPOCH_OFFSET HOST_ID_BITS TIME_BITS SERIAL_BITS TIME_SHIFT SERIAL_SHIFT);
our %EXPORT_TAGS = (
    object => \@object,
    entity => \@entity,
    func => \@func,
    server => \@server,
    storage => \@storage,
);

our @EXPORT_OK = (
    'HAVE_64BITINT',
    'STF_CACHE_DEBUG',
    'STF_DEBUG',
    'STF_TIMER',
    'STF_NGINX_STYLE_REPROXY',
    'STF_NGINX_STYLE_REPROXY_ACCEL_REDIRECT_URL',
    'STF_ENABLE_OBJECT_META',
    'STF_ENABLE_STORAGE_META',
    map { @$_ } values %EXPORT_TAGS
);

1;
