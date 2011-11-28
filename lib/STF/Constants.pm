package STF::Constants;
use strict;
use parent qw(Exporter);

my %constants;
BEGIN {
    %constants = (
        STF_CACHE_DEBUG => $ENV{STF_CACHE_DEBUG} || 0,
        STF_DEBUG => $ENV{STF_DEBUG} || 0,
        STF_TIMER => $ENV{STF_TIMER} || 0,

        OBJECT_ACTIVE => 1,
        OBJECT_INACTIVE => 0,
        ENTITY_ACTIVE => 1,
        ENTITY_INACTIVE => 0,

        EPOCH_OFFSET  => 946684800,
        HOST_ID_BITS  => 16,
        TIME_BITS     => 36,
        Q4M_FUNC_REPLICATE     => 1,
        Q4M_FUNC_DELETE_OBJECT => 2,
        Q4M_FUNC_DELETE_BUCKET => 3,
        Q4M_FUNC_REPAIR_OBJECT => 4,

        STORAGE_MODE_REMOVED => -99,
        STORAGE_MODE_CRASH_RECOVERED => -4,
        STORAGE_MODE_CRASH_RECOVER_NOW => -3,
        STORAGE_MODE_CRASH => -2,
        STORAGE_MODE_DOWN => -1,
        STORAGE_MODE_READ_ONLY => 0,
        STORAGE_MODE_READ_WRITE => 1,
        STORAGE_MODE_RETIRE => 2,
        STORAGE_MODE_MIGRATE_NOW => 3,
        STORAGE_MODE_MIGRATED => 4,
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
    STORAGE_MODE_REMOVED
    STORAGE_MODE_CRASH_RECOVERED
    STORAGE_MODE_CRASH_RECOVER_NOW
    STORAGE_MODE_CRASH                    
    STORAGE_MODE_DOWN
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
    'STF_CACHE_DEBUG',
    'STF_DEBUG',
    'STF_TIMER',
    map { @$_ } values %EXPORT_TAGS
);

1;
