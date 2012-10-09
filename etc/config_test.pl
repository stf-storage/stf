$ENV{STF_TRACE_SQLITE_DBNAME} = "trace.db";
$ENV{STF_HOST_ID} = int(rand(10000));

my %dbopts = ( RaiseError => 1, AutoCommit => 1, mysql_enable_utf8 => 1, AutoInactiveDestroy => 1 );
+{
    %$config,
    'Memcached' => {
        servers => [ split /,/, $ENV{TEST_MEMCACHED_SERVERS} ],
    },
    'Trace::SQLite' => {
        connect_info => [
            "dbi:SQLite:dbname=$ENV{STF_TRACE_SQLITE_DBNAME}",
            undef,
            undef,
            { RaiseError => 1, AutoCommit => 1 }
        ],
    }
};
