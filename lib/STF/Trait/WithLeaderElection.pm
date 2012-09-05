package STF::Trait::WithLeaderElection;
use Mouse::Role;
use Scalar::Util ();
use STF::Log;
use STF::Constants qw(STF_DEBUG);

has election_leader_count => (
    is => 'ro',
    default => 1
);

has election_group_name => (
    is => 'ro',
    default => sub {
        my $self = shift;
        join '.', split /::/, lc blessed $self;
    }
);

sub elect_leader {
    my $self = shift;

    my $name = $self->election_group_name;
    my $limit = $self->election_leader_count;

    my $my_id;
    my $dbh = $self->get('DB::Master');
    $dbh->do(<<EOSQL, undef, $name);
        INSERT INTO election (name) VALUES (?)
EOSQL
    $my_id = $dbh->{mysql_insertid};
    my $guard = Scope::Guard->new(sub {
        if (STF_DEBUG) {
            debugf("Attempting to unregister worker %s from %s", $my_id, $name);
        }
        eval {
            my $dbh = $self->get('DB::Master');
            $dbh->do(<<EOSQL, undef, $my_id);
                DELETE FROM election WHERE id = ?
EOSQL
        };
    });

    # now wait until I become a leader
    my $sth = $dbh->prepare(<<EOSQL);
        SELECT id FROM election WHERE name = ? ORDER BY id ASC LIMIT $limit
EOSQL

    my $loop = 1;
    my $timeout = time() + 60 * 10;
    while ($loop && $timeout > time()) {
        my $id;
        $sth->execute( $name );
        $sth->bind_columns( \($id) );
        while ($sth->fetchrow_arrayref) {
            if ($id eq $my_id) {
                if (STF_DEBUG) {
                    debugf("Elected %s as leader of %s", $my_id, $name);
                }
                return $guard;
            }
        }
        sleep rand 60;
    }
    undef $guard;
}

no Mouse::Role;

1;