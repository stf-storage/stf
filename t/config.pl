
$ENV{STF_HOST_ID} = int(rand(10000));
my $config = require 'etc/config.pl';

my $worker = %{ $config->{'Worker::Drone'} || {} };
if ( $ENV{ STF_LOOP_TYPE } ) {
    $worker->{loop_class} = $ENV{ STF_LOOP_TYPE };
} elsif ( $ENV{ STF_LOOP_CLASS } ) {
    $worker->{loop_class} = $ENV{ STF_LOOP_CLASS };
}

my %dbopts = ( RaiseError => 1, AutoCommit => 1, mysql_enable_utf8 => 1, AutoInactiveDestroy => 1 );
+{
    %$config,
    'Memcached' => {
        servers => [ split /,/, $ENV{TEST_MEMCACHED_SERVERS} ],
    },
    'DB::Master' => [
        $ENV{STF_MASTER_DSN} || $ENV{TEST_STF_DSN},
        undef,
        undef,
        \%dbopts,
    ],
    'DB::Slave' => [
        $ENV{STF_SLAVE_DSN} || $ENV{TEST_STF_DSN},
        undef,
        undef,
        \%dbopts,
    ],
    'DB::Queue' => [
        $ENV{STF_QUEUE_DSN} || $ENV{TEST_STF_QUEUE_DSN},
        undef,
        undef,
        \%dbopts,
    ],

    'Worker::Drone' => {
        %$worker,
        spawn_interval => 0,
    },
};
