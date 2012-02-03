package t::lib::App::Prove::Plugin::StartDatabase;
use strict;
use Test::More;

my $MYSQLD;

sub mysql_dsn_to_options {
    my $dsn = shift;

    my @options;
    my @parts = split /;/, $dsn;
    shift @parts;
    
    foreach my $part ( @parts ) {
        my ($opt, $value) = split /=/, $part, 2;
        $opt =~ s/^mysql_//;
        $opt =~ s/_/-/g;
        push @options, "--$opt=$value";
    };
    diag explain \@options;
    return join " ", @options;
}

sub load {
    diag "Checking for explicit TEST_STF_DSN setup";
    # do we have an explicit mysql somewhere?
    if (my $dsn = $ENV{TEST_STF_DSN}) {
        $ENV{TEST_MYSQL_OPTIONS} = mysql_dsn_to_options( $dsn );
        $ENV{TEST_MYSQL_DSN_OPTIONS} = do {
            $dsn =~ s/^[^;]+;//;
            $dsn;
        };
        return;
    }

    # is there a local mysql up?
    if (system('echo "show databases" | mysql -uroot 2>/dev/null') == 0) {
        diag "Found local (default) mysql running. Nothing to do...";
        $ENV{TEST_MYSQL_OPTIONS} = '-uroot';
        $ENV{TEST_MYSQL_DSN_OPTIONS} = "user=root";
        return;
    }

    # Nothing found. Test::mysqld to the rescue
    diag "No database found. Going to start one via Test::mysqld";
    require Test::mysqld;
    $MYSQLD = Test::mysqld->new(
        ($ENV{TEST_MYSQL_BASEDIR} ?
            ( basedir => $ENV{TEST_MYSQL_BASEDIR} ) : ()),
        my_cnf => {
            "skip-networking" => "",
            "sql-mode" => "STRICT_TRANS_TABLES",
        }
    );

    $ENV{TEST_MYSQL_DSN_OPTIONS} = do {
        my $dsn = $MYSQLD->dsn;
        $dsn =~ s/^[^;]+;//;
        $dsn;
    };
    $ENV{TEST_MYSQL_OPTIONS} = mysql_dsn_to_options( $MYSQLD->dsn );
}

END {
    undef $MYSQLD;
}

1;
