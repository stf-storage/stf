package STF::Environment;
use strict;

BEGIN {
    if ($ENV{ DOTCLOUD_ENVIRONMENT }) {
        load_dotcloud_env();
    }
}

sub load_dotcloud_env {
    require YAML;
    my $env = YAML::LoadFile( $ENV{ DOTCLOUD_ENVIRONMENT_YML } || "/home/dotcloud/environment.yml" );

    # $env comes first, because runtime parameters (%ENV) has
    # higher precedence
    %ENV = (%$env, %ENV);

    my $dbname = uc( $ENV{ STF_DOTCLOUD_DB_SERVICE_NAME } || 'db' );
    my $q_name = uc( $ENV{ STF_DOTCLOUD_QUEUE_SERVICE_NAME } || 'db' );

    $ENV{ STF_MYSQL_DSN } ||= sprintf( 
        "dbi:mysql:dbname=stf;host=%s;port=%d",
        $ENV{ "DOTCLOUD_${dbname}_MYSQL_HOST" },
        $ENV{ "DOTCLOUD_${dbname}_MYSQL_PORT" }
    );
    $ENV{ STF_MYSQL_USERNAME } ||= $ENV{ "DOTCLOUD_${dbname}_MYSQL_LOGIN" };
    $ENV{ STF_MYSQL_PASSWORD } ||= $ENV{ "DOTCLOUD_${dbname}_MYSQL_PASSWORD" };

    $ENV{ STF_QUEUE_DSN } ||= sprintf(
        "dbi:mysql:dbname=stf_queue;host=%s;port=%d",
        $ENV{ "DOTCLOUD_${dbname}_MYSQL_HOST" },
        $ENV{ "DOTCLOUD_${dbname}_MYSQL_PORT" }
    );
    $ENV{ STF_QUEUE_USERNAME } ||= $ENV{ "DOTCLOUD_${dbname}_MYSQL_LOGIN" };
    $ENV{ STF_QUEUE_PASSWORD } ||= $ENV{ "DOTCLOUD_${dbname}_MYSQL_PASSWORD" };
}

1;