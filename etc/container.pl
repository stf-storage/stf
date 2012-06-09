use strict;
use Furl::HTTP;
use String::Urandom;
use Cache::Memcached::Fast;
use Class::Load ();
use STF::API::Bucket;
use STF::API::Config;
use STF::API::DeletedObject;
use STF::API::Entity;
use STF::API::Object;
use STF::API::Storage;
use STF::API::StorageCluster;
use STF::Constants qw(STF_ENABLE_STORAGE_META STF_ENABLE_OBJECT_META);
BEGIN {
    if ( STF_ENABLE_STORAGE_META ) {
        require STF::API::StorageMeta;
    }
    if ( STF_ENABLE_OBJECT_META ) {
        require STF::API::ObjectMeta;
    }
}
use STF::Constants qw(STF_DEBUG STF_TRACE);

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

register Furl => Furl::HTTP->new( timeout => 30 );
register Memcached => sub {
    my $c = shift;
    my $config = $c->get('config');
    Cache::Memcached::Fast->new( $config->{'Memcached'} );
};

my @queue_names;
foreach my $dbkey (qw(DB::Master DB::Queue)) {
    # XXX lazy. We need to know which queue databases are available,
    # so we'll just skim it from the list
    if ( $dbkey =~ /::Queue/) {
        push @queue_names, $dbkey;
    }

    register $dbkey => sub {
        my $c = shift;
        my $config = $c->get('config');
        my $dbh = DBI->connect( @{$config->{$dbkey}} );
        $dbh->{HandleError} = sub {
            our @CARP_NOT = ('STF::API::WithDBI');
            Carp::croak(shift) };
        return $dbh;
    }, { scoped => 1 };
}

register 'API::Object' => sub {
    my $c = shift;
    STF::API::Object->new(
        cache_expires => 86400,
        %{ $c->get('config')->{ 'API::Object' } || {} },
        container => $c,
        urandom => String::Urandom->new( LENGTH => 30, CHARS => [ 'a' .. 'z' ] ),
    );
};

my @api_names = qw(API::Bucket API::Config API::Entity API::DeletedObject API::Storage API::StorageCluster);
if ( STF_ENABLE_STORAGE_META ) {
    push @api_names, 'API::StorageMeta';
}
if ( STF_ENABLE_OBJECT_META ) {
    push @api_names, 'API::ObjectMeta';
}
foreach my $name (@api_names) {
    my $klass = "STF::$name";
    register $name => sub {
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
    my $type = $ENV{ STF_QUEUE_TYPE } || 'Q4M';
    my $klass = "STF::API::Queue::$type";
    Class::Load::load_class($klass)
        if ! Class::Load::is_class_loaded($klass);

    $klass->new(
        cache_expires => 86400,
        %{ $c->get('config')->{ "API::Queue::$type" } || {} },
        container => $c,
        queue_names => \@queue_names,
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
