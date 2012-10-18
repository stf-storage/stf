use Carp ();

my $queue_type = $ENV{STF_QUEUE_TYPE} || 'Q4M';

# Environment variables
# STF_ENABLE_STORAGE_META
# STF_HOST_ID
# STF_DEBUG

+{
    # API::* allows you to configure a lot of detail.
    # Normally you don't need to do extra work here
    # 'API::Object' => {}
    # 'API::Entity' => {},
    # 'API::Storage' => {}
    'API::Notification::Email' => {
        from => 'hello@world.net'
    },

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

    # Session state configuration
    'AdminWeb::Session::State' => {
        path => "/",
        domain => "admin.stf.your.company.com",
        expires => 86400,
        httponly => 1,
        secure => 1,
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
            'Data::Dumper::Concise',
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
    # The Q4M/Schwartz/Resque DB.
    'DB::Queue' => (
        $queue_type eq 'Resque' ?
        +{
            redis => $ENV{STF_REDIS_HOSTPORT},
        } :
        $queue_type eq 'Redis' ?
        +{
            server => $ENV{STF_REDIS_HOSTPORT},
            reconnect => 60,
            every     => 250,
            encoding  => undef
        } :
        [
            $ENV{ STF_QUEUE_DSN } || "dbi:mysql:dbname=stf_queue",
            $ENV{ STF_QUEUE_USERNAME } || "root",
            $ENV{ STF_QUEUE_PASSWORD } || undef,
            {
                AutoCommit => 1,
                AutoInactiveDestroy => 1,
                RaiseError => 1,
                mysql_enable_utf8 => 1,
            }
        ]
    ),
    # Localizer - only used in the AdminWeb
    'Localizer' => {
        localizers => [
            { class => 'Gettext', path => path_to("etc/gettext/*.po") },
        ]
    },
    # The Worker config
    # XXX Need to write more docs here
    'Worker::Drone' => {
        # XXX The number of max workers and the number of workers for
        # each server needs to be configured in a central location, not
        # in these static files. Otherwise, how are we to change the number
        # workers dynamically, when we need to?
        pid_file       => '/tmp/worker-drone.pid',
#        max_workers    => 20,
        spawn_interval => 1,
    },
    'Worker::Replicate' => {
        loop_class     => $queue_type,
    },
    'Worker::DeleteObject' => {
        loop_class     => $queue_type,
    },
    'Worker::DeleteBucket' => {
        loop_class     => $queue_type,
    },
    'Worker::RepairObject' => {
        loop_class     => $queue_type,
    },
    'Worker::ObjectHealth' => {
        loop_class     => $queue_type,
    },
}
