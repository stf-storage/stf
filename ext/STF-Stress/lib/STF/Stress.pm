package STF::Stress;
use strict;
use Class::Load ();
use DBI;
use File::Spec;
use Furl::HTTP;
use Parallel::ForkManager;
use Parallel::Scoreboard;
use POSIX qw(SIGTERM);
use Class::Accessor::Lite
    new => 1,
;

sub run {
    my ($self, $config) = @_;

    my $base = $config->{stf_uri};
    my $connect_info = $config->{connect_info};
    my $dbh = DBI->connect( @$connect_info );
    my $get_tables = $dbh->prepare( <<EOSQL );
        SHOW TABLES
EOSQL
    $get_tables->execute();
    while ( my $row = $get_tables->fetchrow_arrayref() ) {
        $dbh->do( "DROP TABLE $row->[0]" );
    }

    $dbh->do( <<EOSQL );
        CREATE TABLE IF NOT EXISTS history (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            method  CHAR(8) NOT NULL,
            uri     TEXT NOT NULL,
            code    CHAR(3) NOT NULL,
            elapsed FLOAT NOT NULL,
            created_on DATETIME NOT NULL,
            modified_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARACTER SET='utf8'
EOSQL

    $dbh->do( <<EOSQL );
        CREATE TABLE IF NOT EXISTS buckets (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            name TEXT NOT NULL,
            created_on DATETIME NOT NULL,
            modified_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name(255))
        ) ENGINE=InnoDB DEFAULT CHARACTER SET='utf8'
EOSQL

    $dbh->do( <<EOSQL );
        CREATE TABLE IF NOT EXISTS objects (
            sha1 CHAR(40) NOT NULL PRIMARY KEY,
            status INT NOT NULL DEFAULT 0, /* 0 - not ready, -1 deleted, 1 - ready */
            size INT NOT NULL,
            uri  TEXT NOT NULL,
            bucket TEXT NOT NULL,
            created_on DATETIME NOT NULL,
            modified_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
EOSQL

    my $workers = $config->{workers};

    my $scoreboard = Parallel::Scoreboard->new(
        base_dir => "load-$$",
    );
    my $pfm = Parallel::ForkManager->new( scalar @$workers );
    my %children;
    $SIG{INT} = sub {
        local $SIG{INT} = 'IGNORE';
        foreach my $pid (keys %children) {
            kill SIGTERM(), $pid;
        }
    };

    for my $worker ( @$workers ) {
        my $pid = $pfm->start;
        if ( $pid ) {
            $children{$pid}++;
            next;
        }

        eval {
            local %SIG;
            my $klass = sprintf "STF::Stress::Worker::%s", $worker->{klass};
            Class::Load::load_class( $klass );
            my $w = $klass->new(
                scoreboard => $scoreboard,
                stf_uri  => $base,
                $config->{timeout} ? (timeout => $config->{timeout}) : (),
                $config->{connect_info} ? (connect_info => $config->{connect_info}) : (),
                %$worker,
            );
            $w->run;
        };
        warn $@ if $@;
        $pfm->finish;
    }

    $pfm->wait_all_children;

    %children = ();
    $SIG{INT} = 'DEFAULT';

    print STDERR "Deleting all buckets...\n";
    $dbh = DBI->connect( @$connect_info );
    my $get_buckets = $dbh->prepare( <<EOSQL );
        SELECT name FROM buckets 
EOSQL
    $get_buckets->execute();
    my $furl = Furl::HTTP->new;
    while ( my $row = $get_buckets->fetchrow_arrayref() ) {
        $furl->delete( "$base/$row->[0]" );
    }

}

sub analyze {
    my ($self, $dir) = @_;

    my @databases = glob( "$dir/state-*.db" );

    my %total;
    my %children;
    foreach my $db ( @databases ) {
        my $dbh = DBI->connect( "dbi:SQLite:dbname=$db", undef, undef,
            { RaiseError => 1, AutoCommit => 1 } );

        $total{ "Total # of requests" } +=
            $dbh->selectrow_arrayref( "SELECT count(*) FROM requests" )->[0];
        $total{ "Total time spent" } +=
            $dbh->selectrow_arrayref( "SELECT max(timestamp) - min(timestamp) FROM requests " )->[0];
        $total{ "Avg. Elapsed / Request" } +=
            $dbh->selectrow_arrayref( "SELECT avg(elapsed) FROM requests" )->[0];

        my ($pid) = $dbh->selectrow_array( "SELECT DISTINCT pid FROM requests" );
        my %results;
        $children{ $pid } = \%results;

        $results{ "Object size" } =
            $dbh->selectrow_arrayref( "SELECT avg(size) FROM requests" )->[0];
        $results{ "Total # of requests" } =
            $dbh->selectrow_arrayref( "SELECT count(*) FROM requests" )->[0];
        $results{ "Total time" } =
            $dbh->selectrow_arrayref( "SELECT max(timestamp) - min(timestamp) FROM requests" )->[0];
        $results{ "Avg. Elapsed / Request" } =
            $dbh->selectrow_arrayref( "SELECT avg(elapsed) FROM requests" )->[0];
    }

    print "Results (Total):\n";
    while ( my( $key, $value) = each %total ) {
        print "    $key: $value\n";
    }

    while ( my ($pid, $results) = each %children ) {
        print "Results for worker ($pid)\n";
        while ( my( $key, $value) = each %$results ) {
            print "    $key: $value\n";
        }
    }

}

1;

__END__

=head1 FLOW

    * DROP TABLE for all tables (schema may change)
    * start workers
    * foreach bucket, delete
    * collect stats, analyze and print
