package STF::AdminWeb;
use strict;
use Mojo::Base 'Mojolicious';
use STF::AdminWeb::Renderer;
use STF::Context;
use Data::Page;
use HTML::FillInForm::Lite;

has 'context';
has use_reverse_proxy => 0;
has fif => HTML::FillInForm::Lite->new;

sub psgi_app {
    my ($self) = @_;

    require Mojo::Server::PSGI;
    require Plack::Middleware::Session;

    my $app = Mojo::Server::PSGI->new(app => $self)->to_psgi_app;
    my $container = $self->context->container;
    $app = Plack::Middleware::Session->wrap($app,
        store => $container->get("AdminWeb::Session::Store"),
        state => $container->get("AdminWeb::Session::State"),
    );
    if ($self->use_reverse_proxy) {
        require Plack::Middleware::ReverseProxy;
        $app = Plack::Middleware::ReverseProxy->wrap( $app );
    }

    return $app;
}

sub startup {
    my ($self) = @_;

    $ENV{MOJO_HOME} ||= $self->context->home;
    $self->home->detect;

    $self->setup_renderer();
    $self->setup_routes();

    $self->hook(after_render => sub {
        my ($c, $output_ref, $format) = @_;

        if ($format !~ m{^text/x?html$}) {
            return;
        }

        if ($c->req->method eq 'POST') {
            $$output_ref = $self->fif->fill( $output_ref, $c->req);
        } elsif ( my $fdat = $c->stash->{fdat} ) {
            $$output_ref = $self->fif->fill( $output_ref, $fdat );
        }
    });

    $self->helper(get => sub {
        my ($c, $name) = @_;
        $c->app->context->container->get($name);
    });

    $self->helper(pager => sub {
        my ($c, $limit) = @_;
        my $req = $c->req;
        my $p   = int($req->param('p') || 0);
        if ($p <= 0) {
            $p = 1;
        }
        my $pager = Data::Page->new;
        $pager->entries_per_page( $limit );
        $pager->current_page($p);
        $pager->total_entries( $p * $limit );
        return $pager;
    });
}

sub setup_renderer {
    my ($self) = @_;

    my $renderer = $self->renderer;
    $renderer->add_handler(tx => STF::AdminWeb::Renderer->build(
        app => $self,
        %{ $self->context->container->get('config')->{'AdminWeb::Renderer'} || {} },
        function => {
            loc => sub { $self->get('Localizer')->localize(@_) },
        },
    ));
    $renderer->default_handler("tx");
}

sub setup_routes {
    my ($self) = @_;
    my $r = $self->routes;

    unshift @{$r->namespaces}, "STF::AdminWeb::Controller";
    $r->any("/")->to(
        controller => "root",
        action     => "index",
    );

    # API endpoints.
    foreach my $name (qw(storage worker)) {
        $r->get("/api/$name/list")->to(
            controller => $name,
            action     => "api_list",
        );
    }

    # Docs
    $r->get("/doc/*filename")->to(
        controller => "document",
        action     => "view"
    );

    # Object
    $r->get("/object")->to(
        controller => "object",
        action     => "index"
    );
    $r->get("/object/show/:object_id")->to(
        controller => "object",
        action     => "view"
    );
    $r->post("/object/edit/:object_id")->to(
        controller => "object",
        action     => "edit_post"
    );
    $r->get("/object/edit/:object_id")->to(
        controller => "object",
        action     => "edit"
    );
}

1;

__END__

package STF::AdminWeb;
use Mouse;
use HTML::FillInForm::Lite;
use STF::Constants;
use STF::Context;
use STF::AdminWeb::Context;

has context => (
    is => 'rw',
    required => 1,
);

has router => (
    is => 'rw',
    required => 1,
);

has stf_base => (
    is => 'rw'
);

has default_view_class => (
    is => 'rw',
    default => 'Xslate',
);

has use_reverse_proxy => (
    is => 'rw',
    default => 0,
);

has htdocs => (
    is => 'rw',
    required => 1,
);

sub bootstrap {
    my $class = shift;
    my $context = STF::Context->bootstrap(@_);

    # These are the default values
    my $use_reverse_proxy = 
        # if USE_REVERSE_PROXY exists, use that value
        exists $ENV{USE_REVERSE_PROXY} ?  $ENV{USE_REVERSE_PROXY} :
        # if PLACK_ENV is production, then use reverse proxy
        $ENV{PLACK_ENV} eq 'production' ? 1 :
        # otherwise no
        0
    ;
    my $htdocs = File::Spec->catfile( $ENV{STF_HOME} || $ENV{DEPLOY_HOME} || Cwd::cwd(), "htdocs" );

    my $app = STF::AdminWeb->new(
        use_reverse_proxy => $use_reverse_proxy,
        htdocs => $htdocs,
        %{ $context->get('config')->{'AdminWeb'} || {} },
        context => $context,
        router  => $context->get('AdminWeb::Router'),
    );

    return $app;
}

sub to_app {
    my $self = shift;

    my $app = sub {
        my $env = shift;
        $self->handle_psgi($env);
    };

    my $container = $self->context->container;
    require Plack::Middleware::Session;
    $app = Plack::Middleware::Session->wrap($app,
        store => $container->get("AdminWeb::Session::Store"),
        state => $container->get("AdminWeb::Session::State"),
    );
    if ($self->use_reverse_proxy) {
        require Plack::Middleware::ReverseProxy;
        return Plack::Middleware::ReverseProxy->wrap( $app );
    } else {
        require Plack::Middleware::Static;
        return Plack::Middleware::Static->wrap( $app, (
            path => qr{^/static},
            root => $self->htdocs,
        ) );
    }
}

sub handle_psgi {
    my ($self, $env) = @_;

    my $context = $self->context;
    my $rc = STF::AdminWeb::Context->new(
        env => $env,
        container => $context->container,
    );
    my $guard = $context->container->new_scope();
    my $localizer = $context->get('Localizer');

    my $sessions = $rc->session;
    my $lang = $sessions->get('lang') || 'ja';
    $localizer->set_languages( $lang );
    $sessions->set(lang => $lang);

    eval {
        $self->dispatch( $rc, $env );
    };
    if (my $e = $@) {
        if ($e !~ /^stf\.abort/) {
            $self->handle_server_error($rc, $e);
        }
    }

    return $rc->response->finalize();
}

sub handle_not_found {
    my ($self, $c, $message) = @_;

    my $response = $c->response;
    $response->code( 404 );
    $response->content_type( "text/plain" );
    $response->body( $message ) if $message;
}

sub handle_server_error {
    my ($self, $c, $message) = @_;

    my $response = $c->response;
    $response->code( 500 );
    $response->content_type( "text/plain" );
    $response->body( $message ) if $message;
}

sub dispatch {
    my ($self, $context, $env) = @_;

    my $h = $self->router->match( $env );
    if (! $h) {
        # 404
        $self->handle_not_found( $context );
        return;
    }

    $context->match( $h );
    my $action = $h->{action};
    my $controller_class = $h->{controller};
    my $controller = $self->get_component(
        $controller_class, 'STF::AdminWeb::Controller' );

    $controller->execute( $context, $action );
    if (! $context->finished) {
        $self->render( $context, $controller, $action );
    }
}

sub get_component {
    my ($self, $klass, $prefix) = @_;

    if ( $klass !~ s/^\+// ) {
        if (! $prefix) { Carp::croak( "No prefix provided" ) }
        $klass = join '::', $prefix, $klass;
    }

    my $component;
    {
        local $@;
        $component = eval { $self->context->get( $klass ) };
    }
    if ($component) {
        return $component;
    }

    Mouse::Util::load_class($klass) unless
        Mouse::Util::is_class_loaded($klass);

    my $key = $klass;
    $key =~ s/^STF:://;
    my $config = $self->context->config->{$key} || {};

    $component = $klass->new( %$config, app => $self);
    $self->context->container->register( $key => $component );

    return $component;
}

sub render {
    my ($self, $context, $controller, $action) = @_;

    my $stash = $context->stash;
    my $template = $stash->{template} ||
        join( '/', do {
            my @list = ($action);
            unshift @list, $controller->namespace if $controller->namespace;
            @list
        } )
    ;

    my $view_class = $stash->{view_class} ||
        $controller->view_class ||
        $self->default_view_class
    ;
    my $view = $self->get_component( $view_class, 'STF::AdminWeb::View' );
    if (! $view) {
        die "No view found";
    }


    $stash->{c} = $context;
    $stash->{const} = STF::Constants::as_hashref();
    $stash->{stf_base} = $self->stf_base;
    $stash->{session} = $context->session;
    $view->process( $context, $template );
}

no Mouse;

1;
