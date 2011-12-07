package STF::Environment;
use strict;

sub load_dotcloud_env {
    my $file = shift;

    require YAML;
    my $env = YAML::LoadFile( $file );

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

BEGIN {
    my $dotcloud_envfile = $ENV{ DOTCLOUD_ENVIRONMENT } || '/home/dotcloud/environment.yml';
    if (-f $dotcloud_envfile) {
        load_dotcloud_env($dotcloud_envfile);
    }
}

1;