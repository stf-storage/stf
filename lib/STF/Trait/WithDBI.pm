package STF::Trait::WithDBI;
use strict;
use feature 'state';
use parent qw(STF::Trait::WithContainer);
use DBI ();
use Guard ();

sub dbh {
    my ( $self, $key ) = @_;
    $key ||= 'DB::Master';
    $self->get( $key );
}

sub txn_block {
    my ($self, $dbkey, $code, @args) = @_;

    state $txn = sub {
        my ($self, $dbkey, $code, @args) = @_;
        my $dbh = $self->get($dbkey);
        if (! $dbh) {
            Carp::confess("Could not get $dbkey from container");
        }
        $dbh->begin_work;
        my $guard = Guard::guard { 
            eval { $dbh->rollback }
        };
        my @res = $code->($self, @args);
        $dbh->commit;
        $guard->cancel;
        return @res ;
    };
    return $txn->( $self, $dbkey, $code, @args );
}

1;
