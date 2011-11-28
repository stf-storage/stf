use strict;
use Furl::HTTP;
use String::Urandom;
use Cache::Memcached::Fast;
use STF::API::Bucket;
use STF::API::DeletedObject;
use STF::API::Entity;
use STF::API::Object;
use STF::API::Queue;
use STF::API::Storage;
use STF::Constants qw(STF_DEBUG);

register Furl => Furl::HTTP->new( timeout => 30 );
register Memcached => sub {
    my $c = shift;
    my $config = $c->get('config');
    Cache::Memcached::Fast->new( $config->{'Memcached'} );
};

foreach my $dbkey (qw(DB::Master DB::Slave DB::Queue)) {
    register $dbkey => sub {
        my $c = shift;
        my $config = $c->get('config');
        DBI->connect( @{$config->{$dbkey}} );
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

foreach my $name (qw(API::Bucket API::Queue API::Entity API::DeletedObject API::Storage)) {
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
