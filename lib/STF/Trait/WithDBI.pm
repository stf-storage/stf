package STF::Trait::WithDBI;
use strict;
use parent qw(STF::Trait::WithContainer);
use DBI ();
use Guard ();

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
        my $guard = Guard::guard { 
            eval { $dbh->rollback }
        };
        my @res = $txn->($self, @args);
        $dbh->commit;
        $guard->cancel;
        return @res ;
    };
}

1;
