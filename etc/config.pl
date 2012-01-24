use Carp ();

+{
    # Dispatcher settings
    'Dispatcher' => {
        # host_id is used to generate unique object IDs.
        # It is very important to use a UNIQUE value for EACH DISPATCHER
        # INSTANCE. DO NOT USE THE SAME ID! 
        # You've been warned.
        host_id => $ENV{STF_HOST_ID},
    },

    # Used for Admin interface. Change the stf_base URL as appropriate
    'AdminWeb' => {
        stf_base => "http://stf.mycompany.com",
        default_view_class => 'Xslate',
        # Set to true if you want to enable reverse proxy middleware
        # Or, 
        #   1) you can set USE_REVERSE_PROXY environment variable
        #   2) if USE_REVERSE_PROXY isn't set and PLACK_ENV is production, 
        #      then we assume use_reverse_proxy = 1
        #
        # use_reverse_proxy => 1,

        # Path to static files
        # htdocs => path_to( "htdocs" ),
    },
    # Used for Admin interface. You should not need to change this
    'AdminWeb::Router' => {
        routes => path_to("etc/admin/routes.pl"),
    },
    # Used for Admin interface. You should not need to change this
    'AdminWeb::Validator' => {
        profiles => path_to('etc/admin/profiles.pl'),
    },
    # Used for Admin interface. You should not need to change this
    'AdminWeb::View::Xslate' => {
        path => [
            path_to("view"),
            path_to("view", "inc"),
        ],
        module => [
            'STF::Xslate::Utils',
        ],
        suffix => '.tx',
        syntax => 'TTerse',
    },

    # Memcached settings
    Memcached => {
        # Add as many servers as you may have.
        servers => [ '127.0.0.1:11211' ],
        namespace => 'stf.',
        compress_threshold => 100_000,
    },

    # Database settings.
    # The master DB.
    'DB::Master' => [
        $ENV{ STF_MYSQL_DSN } || "dbi:mysql:dbname=stf",
        $ENV{ STF_MYSQL_USERNAME } || "root",
        $ENV{ STF_MYSQL_PASSWORD } || undef,
        {
            AutoCommit => 1,
            AutoInactiveDestroy => 1,
            RaiseError => 1,
            mysql_enable_utf8 => 1,
        }
    ],
    # The Q4M/Schwartz DB.
    'DB::Queue' => [
        $ENV{ STF_QUEUE_DSN } || "dbi:mysql:dbname=stf_queue",
        $ENV{ STF_QUEUE_USERNAME } || "root",
        $ENV{ STF_QUEUE_PASSWORD } || undef,
        {
            AutoCommit => 1,
            AutoInactiveDestroy => 1,
            RaiseError => 1,
            mysql_enable_utf8 => 1,
        }
    ],

    # The Worker config
    # XXX Need to write more docs here
    'Worker::Drone' => {
        pid_file       => '/tmp/worker-drone.pid',
        spawn_interval => 1,
    },
    'Worker::Replicate' => {
        loop_class     => $ENV{ STF_QUEUE_TYPE } || "Q4M",
    },
    'Worker::DeleteObject' => {
        loop_class     => $ENV{ STF_QUEUE_TYPE } || "Q4M",
    },
    'Worker::DeleteBucket' => {
        loop_class     => $ENV{ STF_QUEUE_TYPE } || "Q4M",
    },
    'Worker::RepairObject' => {
        loop_class     => $ENV{ STF_QUEUE_TYPE } || "Q4M",
    },
    'Worker::ObjectHealth' => {
        loop_class     => $ENV{ STF_QUEUE_TYPE } || "Q4M",
    },
}
