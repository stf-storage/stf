package STF::AdminWeb;
use strict;
use feature 'state';
use Mojo::Base 'Mojolicious';
use STF::AdminWeb::Renderer;
use STF::Context;
use Data::Page;
use HTML::FillInForm::Lite;

has 'context';
has use_reverse_proxy => 0;
has fif => sub { HTML::FillInForm::Lite->new };

sub psgi_app {
    my ($self) = @_;

    require Mojo::Server::PSGI;
    require Plack::Middleware::Session;
    require Plack::Session;

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

    $self->hook(around_dispatch => sub {
        my ($next, $c) = @_;
        my $guard = $self->context->container->new_scope();
        my $session = Plack::Session->new($c->req->env);
        my $lang = $session->get('lang') || 'ja';
        my $localizer = $c->get('Localizer');
        $localizer->set_languages( $lang );
        $session->set(lang => $lang);
        $c->stash(
            const => STF::Constants->as_hashref,
            session => $session,
        );
        $next->();
    });
    $self->helper(sessions => sub {
        my $c = shift;
        $c->stash->{session};
    });
    $self->hook(after_render => sub {
        my ($c, $output_ref, $format) = @_;

        if ($format !~ m{^x?html$}) {
            return;
        }

        if ($c->req->method eq 'POST') {
            $$output_ref = $self->fif->fill( $output_ref, $c->req);
        } elsif ( my $fdat = $c->stash->{fdat} ) {
            $$output_ref = $self->fif->fill( $output_ref, $fdat );
        }
    });

    $self->helper(stf_uri => sub {
        my ($c, $bucket, $object) = @_;
        my $config = $self->context->container->get('config')->{'AdminWeb'} || {};
        my $prefix = $config->{stf_base} || "http://changeme";
        $prefix =~ s{/+$}{};
        
        $c->url_for(join "/", $prefix, $bucket->{name}, $object->{name});
    
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

    my $config = $self->context->container->get('config')->{'AdminWeb::Renderer'} || {};
    # Setup static path
    if (my $path = $config->{static}) {
        unshift @{$self->static->paths}, $path;
    }

    my $renderer = $self->renderer;
    $renderer->add_handler(tx => STF::AdminWeb::Renderer->build(
        %config,
        app => $self,
        function => {
            loc => sub { $self->get('Localizer')->localize(@_) },
            error_msgs => Text::Xslate::html_builder(sub {
                my ($result) = @_;

                if (! defined $result) {
                    return '';
                }

                if ( $result->success) {
                    return '';
                }

                my $msgs = $result->msgs;
                return sprintf '<ul class="error">%s</ul>',
                    join '', map { "<li>$_: @{$msgs->{$_}}</li>" }
                        keys %$msgs;
            }),
            nl2br => Text::Xslate::html_builder(sub {
                my $text = shift;
                $text =~ s/\n/<br \/>/;
                $text
            }),
            mode_str => sub {
                state $mode_str = {
                    STF::Constants::STORAGE_MODE_CRASH_RECOVERED() => 'crashed (repair done)',
                    STF::Constants::STORAGE_MODE_CRASH_RECOVER_NOW() => 'crashed (repairing now)',
                    STF::Constants::STORAGE_MODE_CRASH() => 'crashed (need repair)',
                    STF::Constants::STORAGE_MODE_RETIRE() => 'retire',
                    STF::Constants::STORAGE_MODE_MIGRATE_NOW() => 'migrating',
                    STF::Constants::STORAGE_MODE_MIGRATED() => 'migrated',
                    STF::Constants::STORAGE_MODE_READ_WRITE() => 'rw',
                    STF::Constants::STORAGE_MODE_READ_ONLY() => 'ro',
                    STF::Constants::STORAGE_MODE_TEMPORARILY_DOWN() => 'down',
                    STF::Constants::STORAGE_MODE_REPAIR() => 'need repair',
                    STF::Constants::STORAGE_MODE_REPAIR_NOW() => 'repairing',
                    STF::Constants::STORAGE_MODE_REPAIR_DONE() => 'repair done',
                };
                return $mode_str->{$_[0]} || "unknown ($_[0])";
            },
            paginate => Text::Xslate::html_builder(sub {
                my ($uri, $pager) = @_;
                sprintf qq{%s | %s},
                    $pager->previous_page ?
                        sprintf '<a href="%s">Prev</a>',
                        do {
                            my $u = $uri->clone;
                            $u->query->param(p => $pager->previous_page);
                            $u;
                        }
                    :
                    "Prev",
                    $pager->next_page ?
                        sprintf '<a href="%s">Next</a>',
                        do {
                            my $u = $uri->clone;
                            $u->query->param(p => $pager->next_page );
                            $u;
                        }
                    :
                    "Next"
                ;
            }),
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

    $r->get('/setlang')->to(
        controller => 'root',
        action     => "setlang",
    );

    # Docs
    $r->get("/doc/*filename")->to(
        controller => "document",
        action     => "view"
    );

    $r->get("/bucket")->to(cb => sub {
        shift->redirect_to("/bucket/list");
    });
    $r->get("/bucket/list")->to(
        controller => "bucket",
        action     => "list",
    );
    $r->get("/bucket/add")->to(
        controller => "bucket",
        action     => "add",
    );
    $r->post("/bucket/add")->to(
        controller => "bucket",
        action     => "add_post",
    );
    $r->get("/bucket/show/:object_id")->to(
        controller => "bucket",
        action     => "view"
    );
    # XXX Fix URI path
    $r->post("/api/bucket/:object_id/delete.json")->to(
        controller => 'bucket',
        action     => "api_delete",
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
    $r->post("/object/create")->to(
        controller => "object",
        action     => "create_post"
    );
    $r->get("/object/create")->to(
        controller => "object",
        action     => "create",
    );
    $r->post("/object/edit/:object_id")->to(
        controller => "object",
        action     => "edit_post"
    );
    $r->get("/object/edit/:object_id")->to(
        controller => "object",
        action     => "edit",
    );

    foreach my $action (qw(delete repair)) {
        $r->post("/ajax/object/:object_id/$action.json")->to(
            controller => 'object',
            action     => $action,
        );
    }
    # Clusters and storages share pretty much the same
    # URL structure. yay
    foreach my $controller (qw(storage cluster)) {
        $r->get("/$controller")->to(cb => sub {
            shift->redirect_to("/$controller/list");
        });
        $r->get("/$controller/list")->to(
            controller => $controller,
            action     => "list"
        );
        $r->get("/$controller/entities/:object_id")->to(
            controller => $controller,
            action     => "entities",
        );
        $r->get("/$controller/show/:object_id")->to(
            controller => $controller,
            action     => "view"
        );
        $r->post("/$controller/add")->to(
            controller => $controller,
            action     => "add_post"
        );
        $r->get("/$controller/add")->to(
            controller => $controller,
            action     => "add"
        );
        $r->post("/$controller/edit/:object_id")->to(
            controller => $controller,
            action     => "edit_post"
        );
        $r->get("/$controller/edit/:object_id")->to(
            controller => $controller,
            action     => "edit"
        );
        $r->post("/$controller/delete/:object_id")->to(
            controller => $controller,
            action     => "delete_post"
        );
    }
    $r->get("/cluster/free")->to(
        controller => "cluster",
        action     => "storage_free",
    );

    $r->get('/config')->to(
        controller => 'config',
        action     => 'index',
    );
    $r->get('/config/notification')->to(
        controller => 'config',
        action     => 'notification',
    );
    $r->post('/config/notification/rule/add')->to(
        controller => 'config',
        action     => 'notification_rule_add',
    );
    $r->get('/config/worker')->to(cb => sub {
        shift->redirect_to("/config/worker/list");
    });
    $r->get('/config/worker/list')->to(
        controller => 'config',
        action     => 'worker_list',
    );
    $r->get('/config/worker/:worker_name')->to(
        controller => 'config',
        action     => 'worker',
    );
    $r->post('/config/update')->to(
        controller => 'config',
        action     => 'update',
    );
    $r->post('/ajax/config/reload.json')->to(
        controller => 'config',
        action     => 'reload',
    );
    $r->post('/ajax/notification/rule/toggle.json')->to(
        controller => 'config',
        action     => 'notification_rule_toggle',
    );
    $r->post('/ajax/notification/rule/delete.json')->to(
        controller => 'config',
        action     => 'notification_rule_delete',
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
