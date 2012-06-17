package STF::Trace::SQLite;
use Mouse;

use DBD::SQLite;

has connect_info => (
    is => 'rw',
    isa => 'ArrayRef',
);

has dbh => (
    is => 'rw',
    builder => \&_initialize_dbh
);

sub _initialize_dbh {
    my $self = shift;
    my $connect_info = $self->connect_info;
    if (! $connect_info) {
        require Carp;
        Carp::croak("No connect_info and no dbh provided");
    }
    my $dbh = DBI->connect( @$connect_info );

    $dbh->do(<<EOSQL);
        CREATE TABLE IF NOT EXISTS trace_log (
            id INTEGER AUTO_INCREMENT PRIMARY KEY,
            name TEXT,
            package TEXT,
            file TEXT,
            line INTEGER,
            sub TEXT,
            message TEXT,
            created_on INTEGER
        )
EOSQL
    $dbh;
}

sub trace {
    my ($self, $name, %args) = @_;

    my ($pkg, $file, $line, $sub) = caller(1);
    $self->dbh->do(<<EOSQL, undef, $name, $pkg, $file, $line, $sub, $args{message}, time());
        INSERT INTO trace_log (name, package, file, line, sub, message, created_on) VALUES (?, ?, ?, ?, ?, ?, ?)
EOSQL
}

sub clear {
    my $self = shift;
    $self->dbh->do("DELETE FROM trace_log");
}

no Mouse;

1;
