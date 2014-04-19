package STF::AdminWeb::Renderer;
use Mojo::Base 'Mojolicious::Renderer';
use File::Spec ();
use Mojo::Loader;
use Text::Xslate ();

has 'xslate';

sub build {
    my $self = shift->new(@_);
    $self->_init(@_);
    return sub { $self->render(@_) };
}

sub _init {
    my ($self, %args) = @_;

    my $app = delete $args{mojo} || delete $args{app};
    my $cache_dir = $args{cache_dir};
    my $path = $args{path};
    if (! $path || scalar(@$path) < 1) {
        $path = [ $app->home->rel_dir('templates') ];
    }

    if ($app) {
        if (! $cache_dir) {
            $cache_dir = $app->home->rel_dir('tmp/compiled_templates');
        }

        push @$path, Mojo::Loader->new->data(
            $app->renderer->classes->[0],
        );
    } else {
        if (! $cache_dir) {
            $cache_dir = File::Spec->tmpdir;
        }
    }

    my %config = (
        cache_dir => $cache_dir,
        path      => $path,
        syntax    => 'TTerse',
        %args,
    );

    my $xslate = $self->build_xslate(\%config);
    $self->xslate($xslate);

    return $self;
}

sub build_xslate {
    my ($self, $config) = @_;
    Text::Xslate->new($config);
}

sub render {
    my ($self, $renderer, $c, $output, $options) = @_;

    my $name = $c->stash->{'template_name'}
        || $renderer->template_name($options);
    my %params = (%{$c->stash}, c => $c);

    eval {
        $$output = $self->xslate->render($name, \%params);
    };
    if (my $err = $@) {
        $c->app->log->error(qq(Template error in "$name": $err));
        $$output = '';
        return 0;
    };

    return 1;
}


1;

