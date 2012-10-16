package STF::AdminWeb::Context;
use Mouse;
use Data::Page;
use URI;
use URI::Escape;
# XXX For some reason Xslate + URI(::Escape) is giving me grief.
# Pre-loading these seems to fix the problem
use URI::_server;
use URI::_generic;
use URI::_query;
use Plack::Request;
use Plack::Session;

with 'STF::Trait::WithContainer';

has finished => (
    is => 'rw',
    default => 0,
);

has stash => (
    is => 'rw',
    default => sub { +{} }
);

has match => (
    is => 'rw',
);

has request => (
    is => 'rw',
    required => 1,
);

has response => (
    is => 'rw',
    lazy => 1,
    default => sub { $_[0]->request->new_response(200) },
);

has session => (
    is => 'rw',
    lazy => 1,
    default => sub { Plack::Session->new($_[0]->request->env) }
);

sub BUILDARGS {
    my ($class, %args) = @_;

    if (my $env = delete $args{env}) {
        $args{request} = Plack::Request->new( $env );
    }
    return \%args;
}

sub redirect {
    my ($self, $uri) = @_;
    my $res = $self->response;
    $res->status(302);
    $res->header( Location => $uri );
    $self->abort;
}

sub abort {
    die "stf.abort";
}

sub stf_uri {
    my ($self, $bucket, $object) = @_;

    my $object_name = $object->{name};
    $object_name =~ s/^\///;
    return join
        '/',
        $self->container->get('API::Config')->load_variable('stf.global.public_uri'),
        $bucket->{name},
        $object->{name},
    ;
}

sub uri_for {
    my( $self, @args ) = @_;
    # Plack::App::URLMap

    my $req = $self->request;
    my $uri = $req->base;
    my $params =
        ( scalar @args && ref $args[$#args] eq 'HASH' ? pop @args : {} );
    my @path = split '/', $uri->path;
    unless ( $args[0] =~ m{^/} ) {
        push @path, split( '/', $req->path_info );
    }
    push @path, @args;
    my $path = join '/', @path;
    $path =~ s|/{2,}|/|g; # xx////xx  -> xx/xx
    $uri->path( $path );
    $uri->query_form( $params );
    return $uri;
}

sub pager {
    my ($self, $limit) = @_;
    my $req = $self->request;
    my $p   = int($req->param('p') || 0);
    if ($p <= 0) {
        $p = 1;
    }
    my $pager = Data::Page->new;
    $pager->entries_per_page( $limit );
    $pager->current_page($p);
    $pager->total_entries( $p * $limit );
    return $pager;
}

1;