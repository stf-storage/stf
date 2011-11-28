package STF::AdminWeb::Context;
use strict;
use Data::Page;
use URI;
use URI::Escape;
# XXX For some reason Xslate + URI(::Escape) is giving me grief.
# Pre-loading these seems to fix the problem
use URI::_server;
use URI::_generic;
use URI::_query;
use Plack::Request;
use Class::Accessor::Lite
    rw => [ qw(
        container
        finished
        match
        request
        stash
    ) ]
;

sub new {
    my ($class, %args) = @_;
    bless {
        %args,
        request => Plack::Request->new( delete $args{env} ),
        stash => {},
        finished => 0,
    }, $class;
}

sub get {
    my ($self, $name) = @_;
    $self->container->get($name);
}

sub response {
    my $self = shift;
    $self->{response} ||= $self->request->new_response(200);
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
        $self->container->get('config')->{AdminWeb}->{stf_base},
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
    my $p   = int($req->param('p'));
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