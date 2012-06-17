package STF::Trait::WithDBI;
use Mouse::Role;

with 'STF::Trait::WithContainer';

use DBI ();
use Scope::Guard ();

sub dbh {
    my ( $self, $key ) = @_;
    $key ||= 'DB::Master';
    $self->get( $key );
}

# Creates a reusable coderef bound to '$self', '$txn' (the actual code invoked)
# and '$dbkey'. $dbkey defaults to DB::Master

sub txn_block {
    my ($self, $txn, $dbkey) = @_;

    $dbkey ||= 'DB::Master';
    return sub {
        my (@args) = @_;
        my $dbh = $self->get($dbkey);
        if (! $dbh) {
            Carp::confess("Could not get $dbkey from container");
        }
        $dbh->begin_work;
        my $guard = Scope::Guard->new(sub {
            eval { $dbh->rollback }
        });
        my @res = $txn->($self, @args);
        $dbh->commit;
        $guard->dismiss;
        return @res ;
    };
}

no Mouse::Role;

1;
