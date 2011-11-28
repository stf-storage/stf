use strict;
use Plack::Builder;
use STF::AdminWeb;

my $app = STF::AdminWeb->bootstrap;
builder {
    my $use_proxy;
    if ( exists $ENV{ USE_REVERSE_PROXY } ) {
        $use_proxy = $ENV{ USE_REVERSE_PROXY };
    } elsif ( $ENV{ PLACK_ENV } eq 'prodution' ) {
        $use_proxy = 1;
    } else {
        $use_proxy = 0;
    }

    if( $use_proxy ){
        enable 'ReverseProxy';
    } else {
        my $home = $ENV{DEPLOY_HOME} || do { require Cwd; Cwd::cwd() };
        enable "Plack::Middleware::Static",
            path => qr{^/static},
            root => File::Spec->catdir($home, "htdocs"),
        ;
    }

    $app->to_app;
};
