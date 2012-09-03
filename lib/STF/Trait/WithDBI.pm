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

no Mouse::Role;

1;
