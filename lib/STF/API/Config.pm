package STF::API::Config;
use Mouse;
use Scope::Guard ();

with 'STF::API::WithDBI';

sub remove {
    my ($self, $varname) = @_;
    my $table = $self->table;
    $self->get('DB::Master')->do(<<EOSQL, undef, $varname);
        DELETE FROM $table WHERE varname = ?
EOSQL
}

sub set {
    my ($self, @args) = @_;

    my $dbh = $self->get('DB::Master');
    $dbh->begin_work;
    my $guard = Scope::Guard->new(sub {
        $dbh->rollback();
    });
    while ( my ($key, $value) = splice @args, 0, 2 ) {
        if (! defined $value || length $value <= 0 ) {
            $self->delete( { varname => $key } );
        } else {
            $self->create({
                varname => $key,
                varvalue => $value,
            }, { prefix => "REPLACE INTO" });
        }
    }
    $dbh->commit;
    $guard->dismiss;
}

has loaders => (
    is => 'ro',
    default => sub { +{
        default => sub {
            my ($self, $name) = @_;
            my $vars = $self->load_variables($name);
            my $locals = $self->local_variables->{$name} || {};
            my %config = ( %$vars, %$locals );
            wantarray ? %config : \%config;
        }
    } }
);

has local_variables => (
    is => 'rw',
    default => sub { +{} }
);

sub register_loader {
    my ($self, $name, $cb) = @_;
    return $self->loaders->{$name} = $cb;
}

sub load_config {
    my ($self, $name, $component, $key) = @_;

    foreach my $loader_name ( $name, "default" ) {
        my $loader = $self->loaders->{ $loader_name };
        next unless $loader;
        return $loader->($self, $name, $component, $key);
    }
    return ();
}

sub load_variables {
    my ($self, $var_key, $component, $key) = @_;

    my $globals = $self->load_global_variables( $var_key );
    my $hosts   = $self->load_component_variables( $var_key, $component, $key );
    my %variables = ( %$globals, %$hosts );
    return wantarray ? %variables : \%variables;
}

sub load_global_variables {
    my ($self, $key) = @_;
    my $prefix = "stf.global.";
    $self->load_variables_with_prefix( $key, $prefix );
}

sub load_component_variables {
    my ($self, $var_key, $component, $key) = @_;

    $component ||= $ENV{ STF_COMPONENT_NAME } ||
        Carp::croak("No component name given to load_component_variables() and STF_COMPONENT_NAME was not defined");

    # STF_HOST_ID is here for back compat
    $key       ||= $ENV{ STF_COMPONENT_KEY } || $ENV{ STF_HOST_ID };
    if (! $key) {
        Carp::carp("No component key given to load_component_variables() and STF_COMPONENT_KEY was not defined");
        return wantarray ? () : {};
    }

    my $prefix = sprintf "stf.%s[%s].", $component, $key;
    $self->load_variables_with_prefix( $var_key, $prefix );
}

sub load_variable {
    my ($self, $key) = @_;

    my $dbh = $self->get('DB::Master');
    my ($value) = $dbh->selectrow_array( <<EOSQL, undef, $key );
        SELECT varvalue FROM config WHERE varname = ?
EOSQL
    return $value;
}

sub load_variables_raw {
    my ($self, $term, $tlen) = @_;

    my $dbh = $self->get('DB::Master');
    my $sth;

    if ($tlen) {
        $sth = $dbh->prepare(<<EOSQL);
            SELECT SUBSTRING(varname, ?), varvalue FROM config WHERE varname LIKE ?
EOSQL
        $sth->execute( $tlen, $term );
    } else {
        $sth = $dbh->prepare(<<EOSQL);
            SELECT varname, varvalue FROM config WHERE varname LIKE ?
EOSQL
        $sth->execute( $term );
    }

    my (%variables, $varname, $varvalue);
    $sth->bind_columns( \($varname, $varvalue) );
    while ( $sth->fetchrow_arrayref ) {
        $variables{$varname} = $varvalue;
    }
    $sth->finish;
    return wantarray ? %variables : \%variables;
}

sub load_variables_with_prefix {
    my ($self, $key, $prefix) = @_;
    my $tlen = length("$prefix$key.") + 1;
    my $term = "$prefix$key.%";
    $self->load_variables_raw( $term, $tlen );
}
    
no Mouse;

1;

__END__

=head1 SYNOPSIS

    use STF::API::Config;

    my $config = STF::API::Config->new(
        container => $c
    );

    my $object_api = STF::API::Object->new( 
        $config->load_config( "API::Object" )
    );

=head1 VARIABLES

=cut


