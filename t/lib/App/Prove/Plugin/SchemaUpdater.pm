package t::lib::App::Prove::Plugin::SchemaUpdater;
use strict;
use warnings;
use Test::More;

sub run { system(@_)==0 or die "Cannot run: @_\n-- $!\n"; }

sub get_branch {
    my ($branch) = 
        grep { s/^[\*]\s+// }
        split /\n/, `git branch --no-color 2> /dev/null`
    ;
    if (! $branch) {
        $branch = "no_branch";
    }
    $branch =~ s/\n//;
    $branch =~ s/[^A-Za-z0-9_]/_/g;
    $branch;
}

sub create_database {
    my ($name) = @_;
    diag("CREATE DATABASE $name");
    run("mysqladmin $ENV{TEST_MYSQL_OPTIONS} create $name");
}

sub drop_database {
    my ($name) = @_;
    diag("DROP DATABASE $name");
    run("mysqladmin $ENV{TEST_MYSQL_OPTIONS} --force drop $name");
}
sub copy_database {
    my ($master, $branch_db) = @_;
    diag("COPY DATABASE $master to $branch_db");
    run("mysqldump $ENV{TEST_MYSQL_OPTIONS} --opt -R -d $master | mysql $ENV{TEST_MYSQL_OPTIONS} $branch_db");
}

sub has_database {
    my ($name) = @_;
    my $cmd = "echo 'show databases' | mysql $ENV{TEST_MYSQL_OPTIONS} | perl -ne 'print if /^$name\$/' | wc -l";
    diag $cmd;
    return (`$cmd` =~ /1/);
}

sub create_master {
    my ($target) = @_;
    create_database( $target );

    # XXX hack for queue database, which needs to switch between
    # the database schema
    my $file;
    if ( $target eq 'stf_queue' ) {
        $file = sprintf "misc/stf_%s.sql", lc( $ENV{ STF_QUEUE_TYPE } || "Q4M" );
    } else {
        $file = "misc/$target.sql";
    }

    diag("Running $file");
    run("mysql $ENV{TEST_MYSQL_OPTIONS} $target < $file");

    my $fixture = "misc/${target}_master.sql";
    if ( -f $fixture ) {
        run("mysql $ENV{TEST_MYSQL_OPTIONS} $target < $fixture");
    }

}

sub filter_dumpdata {
    my $data = join "", @_;
    $data =~ s{^/\*.*\*/;$}{}gm;
    $data =~ s{^--.*$}{}gm;
    $data =~ s{^\n$}{}gm;
    $data =~ s{ AUTO_INCREMENT=\d+}{}g;
    $data;
}
sub changed_database {
    my ($master, $branch_db) = @_;
    my $orig = filter_dumpdata(`mysqldump $ENV{TEST_MYSQL_OPTIONS} --opt -R -d $master`);
    my $test = filter_dumpdata(`mysqldump $ENV{TEST_MYSQL_OPTIONS} --opt -R -d $branch_db`);
    return ($orig ne $test);
}

sub load {
    my $branch = get_branch;

    $ENV{TEST_MYSQL_OPTIONS} ||= '-uroot';
    diag "Setting up database for branch $branch";

    my @test_dsn;
    my @databases = qw( stf );
    if ($ENV{STF_QUEUE_TYPE} !~ /^Re(dis|sque)$/) {
        push @databases, 'stf_queue';
    }
    for my $master ( @databases ) {
        if (! has_database( $master ) ) {
            create_master( $master );
        }

        my $branch_db = "${master}_test_${branch}";
        if (has_database($branch_db)) {
            if (changed_database($master, $branch_db)) {
                drop_database($branch_db);
                create_database($branch_db);
                copy_database($master, $branch_db);
            } else {
                diag("No need to update $branch_db");
            }
        } else {
            create_database($branch_db);
            copy_database($master, $branch_db);
        }

        $ENV{ ($master eq 'stf') ? "STF_MYSQL_DSN" : "STF_QUEUE_DSN" } =
            "dbi:mysql:dbname=$branch_db;$ENV{TEST_MYSQL_DSN_OPTIONS}";
    }
}

1;