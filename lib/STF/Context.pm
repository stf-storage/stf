package STF::Context;
use Mouse;
use Carp ();
use Cwd ();
use File::Basename ();
use File::Spec;
use STF::Constants qw(STF_DEBUG);
use STF::Container;
use STF::Log;
use STF::Utils ();

with 'STF::Trait::WithContainer';

has config => (
    is => 'rw'
);

has container => (
    is => 'rw'
);

has home => (
    is => 'rw',
    default => sub {
        return
            $ENV{STF_HOME} ||
            $ENV{DEPLOY_HOME} ||
            Cwd::cwd() ||
            # XXX If you got here, then something is totally whacked.
            die "Could not figure out where STF home is, even using Cwd::cwd()"
        ;
    }
);

sub bootstrap {
    my ($class, %args) = @_;
    my $self = $class->new();

    my $config_file = 
        $args{config} ? (
            File::Spec->file_name_is_absolute( $args{config} ) ?
                $args{config} :
                $self->path_to( $args{config} )
        ) :
        $ENV{ STF_CONFIG } ? (
            File::Spec->file_name_is_absolute( $ENV{ STF_CONFIG } ) ?
                $ENV{ STF_CONFIG } :
                $self->path_to( $ENV{ STF_CONFIG } )
        ) : $self->path_to( 'etc', 'config.pl' )
    ;
    my $container_file = 
        $args{container} ? (
            File::Spec->file_name_is_absolute( $args{container} ) ?
                $args{container} :
                $self->path_to( $args{container} )
        ) :
        $ENV{ STF_CONTAINER } ? (
            File::Spec->file_name_is_absolute( $ENV{ STF_CONTAINER } ) ?
                $ENV{ STF_CONTAINER } :
                $self->path_to( $ENV{ STF_CONTAINER } )
        ) : $self->path_to( 'etc', 'container.pl' )
    ;
    $self->load_config( $config_file );
    $self->load_container( $container_file );
    return $self;
}

sub path_to {
    my $self = shift;
    return File::Spec->catfile($self->home, @_);
}

sub load_config {
    my ($self, $file) = @_;

    my $result = {};
    foreach my $f (STF::Utils::applyenv($file)) {
        next unless -f $f;
        my ($config) = $self->load_file(
            $f => (
                path_to => sub { $self->path_to(@_) }
            )
        );
        $result = STF::Utils::merge_hashes($result, $config);
    }
    $self->config( $result );
}

sub load_container {
    my ($self, $file) = @_;

    my $container = STF::Container->new;
    $container->register(config => $self->config);

    foreach my $f (STF::Utils::applyenv($file)) {
        next unless -f $f;
        $self->load_file(
            $f => (
                register => sub (@) { $container->register(@_) }
            )
        );
    }
    $self->container($container);
}

sub load_file {
    my ($self, $file, %args) = @_;

    local $STF::Log::PREFIX = "Context";

    my $path = Cwd::abs_path($file);
    if (! $path) {
        Carp::croak("$file does not exist");
    }

    if (! -f $path) {
        Carp::confess("$path does not exist, or is not a file");
    }

    if ($INC{$path} && $self->{loaded_paths}{$path}++) {
        if (STF_DEBUG) {
            debugf("Already loaded %s, skipping, $path");
        }
        return;
    }
    delete $INC{$path};
    my $pkg = join '::',
        map { my $e = $_; $e =~ s/[\W]/_/g; $e }
        grep { length $_ } (
            'STF',
            'Context',
            File::Spec->splitdir(File::Basename::dirname($path)),
            File::Basename::basename($path)
        )
    ;

    if (STF_DEBUG) {
        debugf("Loading file %s", $path);
    }

    {
        no strict 'refs';
        no warnings 'redefine';
        while ( my ($method, $code) = each %args ) {
            *{ "${pkg}::${method}" } = $code;
        }
    }

    my $code = sprintf <<'EOM', $pkg, $file;
        package %s;
        require '%s';
EOM
    my @ret = eval $code;
    die if $@;
    return @ret;
}

1;