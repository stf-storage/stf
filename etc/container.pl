use strict;
use DBIx::DSN::Resolver::Cached;
use Furl::HTTP;
use String::Urandom;
use Cache::Memcached::Fast;
use JSON ();
use STF::Constants qw(
    STF_ENABLE_STORAGE_META
    STF_ENABLE_OBJECT_META
    STF_DEBUG
    STF_TRACE
);

if (STF_TRACE) {
    # The tracer is a special mechanism to trace activities from stf.
    # 
    register 'Trace' => sub {
        my $c = shift;
        my $config = $c->get('config');
        require STF::Trace::SQLite;
        STF::Trace::SQLite->new( $config->{'Trace::SQLite'} );
    };
}

register JSON => JSON->new->utf8;
register Furl => Furl::HTTP->new( timeout => 30 );
register Localizer => sub {
    my $c = shift;

    my $config = $c->get('config');
    require Data::Localize;
    my $dl = Data::Localize->new(auto => 1);
    foreach my $loc ( @{ $config->{ Localizer }->{ localizers } } ) {
        $dl->add_localizer( %$loc );
    }
    $dl->set_languages('ja', 'en');
    return $dl;
};
register Memcached => sub {
    my $c = shift;
    my $config = $c->get('config');
    Cache::Memcached::Fast->new( $config->{'Memcached'} );
};
register DSNResolver => DBIx::DSN::Resolver::Cached->new(
    ttl => 30,
    negative_ttl => 5
);

my $register_dbh = sub {
    my ($key) = @_;
    register $key => sub {
        my $c = shift;
        my $config = $c->get('config');

        my $resolver = $c->get('DSNResolver');

        my @connect_info = @{$config->{$key}}
        my $dsn = $resolver->resolv($connect_info[0])
        $connect_info[0] = $dsn;

        my $dbh = DBI->connect(@connect_info);
        $dbh->{HandleError} = sub {
            our @CARP_NOT = ('STF::API::WithDBI');
            Carp::croak(shift) };
        return $dbh;
    }, { scoped => 1 };
};
my $register_resque = sub {
    my ($key) = @_;
    register $key => sub {
        my $c = shift;
        my $config = $c->get('config');
        my $resque = Resque->new(%{$config->{$key}});
        return $resque;
    }, { scoped => 1 };
};
my $register_redis = sub {
    my ($key) = @_;
    register $key => sub {
        my $c = shift;
        my $config = $c->get('config');
        my $redis = Redis->new(%{$config->{$key}});
        return $redis;
    }, { scoped => 1 };
};

$register_dbh->('DB::Master');

my $queue_type = $ENV{ STF_QUEUE_TYPE } || 'Q4M';
my @queue_names =
    exists $ENV{STF_QUEUE_NAMES} ? split( /\s*,\s*/, $ENV{STF_QUEUE_NAMES} ) :
    qw(DB::Queue)
;

foreach my $dbkey (@queue_names) {
    if ($queue_type eq 'Resque') {
        require Resque;
        $register_resque->($dbkey);
    } elsif ($queue_type eq 'Redis') {
        require Redis;
        $register_redis->($dbkey);
    } else {
        $register_dbh->($dbkey);
    }
}

require STF::API::Object;
register 'API::Object' => sub {
    my $c = shift;
    STF::API::Object->new(
        cache_expires => 86400,
        %{ $c->get('config')->{ 'API::Object' } || {} },
        container => $c,
        urandom => String::Urandom->new( LENGTH => 30, CHARS => [ 'a' .. 'z' ] ),
    );
};

my @api_names = qw(
    API::Bucket
    API::Config
    API::Entity
    API::DeletedObject
    API::Notification
    API::NotificationRule
    API::Storage
    API::StorageCluster
    API::WorkerInstances
);

if ( STF_ENABLE_STORAGE_META ) {
    require STF::API::StorageMeta;
    push @api_names, 'API::StorageMeta';
}
if ( STF_ENABLE_OBJECT_META ) {
    require STF::API::ObjectMeta;
    push @api_names, 'API::ObjectMeta';
}

foreach my $name (@api_names) {
    my $klass = "STF::$name";
    register $name => sub {
        Mouse::Util::load_class($klass)
            if ! Mouse::Util::is_class_loaded($klass);
        my $c = shift;
        $klass->new(
            cache_expires => 86400,
            %{ $c->get('config')->{ $name } || {} },
            container => $c,
       );
   };
}

# Our queue may be switched
register "API::Queue" => sub {
    my $c = shift;
    my $klass = "STF::API::Queue::$queue_type";
    Mouse::Util::load_class($klass)
        if ! Mouse::Util::is_class_loaded($klass);

    $klass->new(
        cache_expires => 86400,
        %{ $c->get('config')->{ "API::Queue::$queue_type" } || {} },
        container => $c,
        queue_names => \@queue_names,
    );
};

register 'AdminWeb::Session::Store' => sub {
    my $c = shift;
    my $config = $c->get('config');
    require Plack::Session::Store::Cache;
    return Plack::Session::Store::Cache->new(
        cache => $c->get('Memcached')
    );
};

register 'AdminWeb::Session::State' => sub {
    my $c = shift;
    my $config = $c->get('config');
    require Plack::Session::State::Cookie;
    return Plack::Session::State::Cookie->new(
        %{ $config->{'AdminWeb::Session::State'} }
    );
};

register 'AdminWeb::Validator' => sub {
    require STF::DFV;
    my $c = shift;
    my $config = $c->get('config');
    my $dfv = STF::DFV->new($config->{'AdminWeb::Validator'}->{profiles});
    $dfv->container( $c );
    return $dfv;
};

register 'AdminWeb::Router' => sub {
    my $c = shift;
    require $c->get('config')->{'AdminWeb::Router'}->{routes};
};

my @notifiers = qw(
    Ikachan
    Email
);
foreach my $notifier (@notifiers) {
    my $key = "API::Notification::$notifier";
    my $klass = "STF::$key";
    register $key => sub {
        Mouse::Util::load_class($klass)
            if ! Mouse::Util::is_class_loaded($klass);
        my $c = shift;
        $klass->new( %{ $c->get('config')->{$key} || {} }, container => $c );
    };
}

"DONE";
