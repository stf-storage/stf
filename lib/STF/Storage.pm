package STF::Storage;
use Mouse;
use Cwd ();
use Plack::Middleware::ConditionalGET;
use Plack::Request;
use File::Basename ();
use File::Copy ();
use File::Spec ();
use STF::Constants qw(STF_DEBUG STF_TIMER);
use STF::Utils ();

has root => (
    is => 'ro',
    required => 1,
    default => sub { Cwd::cwd() }
);

has fileapp => (
    is => 'ro',
    required => 1,
    lazy => 1,
    builder => sub {
        my $self = shift;
        require Plack::App::File;
        Plack::App::File->new( root => $self->root );
    }
);

sub to_app {
    my $self = shift;
    my $app = sub { $self->process(@_) };
    $app = Plack::Middleware::ConditionalGET->wrap($app);
    return $app;
}

sub process {
    my ($self, $env) = @_;

    my $method = $env->{REQUEST_METHOD};

    if (my $fileapp = $self->fileapp) {
        if ( $method eq 'GET' || $method eq 'HEAD' ) {
            my $res = $self->fileapp->call( $env );
            return $res;
        }
    }

    my $req = Plack::Request->new($env);
    my $res;
    eval {
        if ($method eq 'PUT') {
            $res = $self->put_object( $req );
        } elsif ($method eq 'DELETE') {
            $res = $self->delete_object( $req );
        } else {
            $res = $req->new_response(400, [], []);
        }
    };
    if ($@) {
        warn $@;
        $res = $req->new_response(500, [], []);
    }
    return $res->finalize;
}

sub put_object {
    my ($self, $req) = @_;

    my $timer;
    if ( STF_TIMER ) {
        $timer = STF::Utils::timer_guard();
    }

    my $dest = File::Spec->catfile( $self->root, $req->path );

    if (STF_DEBUG) {
        print STDERR "[   Backend] Recieved PUT $dest\n";
    }
    my $dir  = File::Basename::dirname( $dest );
    if (! -e $dir ) {
        if (! File::Path::make_path( $dir ) || ! -d $dir ) {
            my $e = $!;
            if ( STF_DEBUG ) {
                printf STDERR "[   Backend] Failed to createdir %s: %s\n",
                    $dir, $e
                ;
            }
            return $req->new_response( 500, [], [ "Failed to create dir @{[ $req->path ]}: $e" ] );
        }
    }

    my $out = File::Temp->new(UNLINK => 1);
    $out->autoflush(1);
    my $fh = $req->input;
    my $cl = $req->content_length;

    # We want to be able to gulp most requests in one shot.
    # Our max object size is 2MB, so be it
    my $readsize = 2 * (1024 ** 2);
    my $read = 0;
    while ( $cl > $read ) {
        my $bytes = read $fh, my ($buf), $readsize;
        if ($bytes == 0) {
            die "Reached EOF?!";
        }
        $read += $bytes;
        print $out $buf;
    }

    $out->flush;

    File::Copy::copy( $out->filename, $dest )
        or die "Failed to rename $out to $dest: $!";

    if ( my $timestamp = $req->header('X-STF-Object-Timestamp') ) {
        # XXX make sure that this is a numeric value ?
        utime $timestamp, $timestamp, $dest;
        if ( STF_DEBUG ) {
            printf STDERR "[   Backend] Set file %s timestamp to %d (%s)\n",
                $dest,
                $timestamp,
                scalar localtime $timestamp,
            ;
        }
    }

    if ( STF_DEBUG ) {
        printf STDERR "[   Backend] Successfully created %s (%d bytes)\n",
            $dest, $read;
    }
    return $req->new_response( 201, [], [] );
}

sub delete_object {
    my ($self, $req) = @_;

    my $timer;
    if ( STF_TIMER ) {
        $timer = STF::Utils::timer_guard();
    }

    my $dest = File::Spec->catfile( $self->root, $req->path );
    if ( STF_DEBUG ) {
        printf STDERR "[   Backend] Request to DELETE $dest\n";
    }

    if (! -f $dest) {
        if ( STF_DEBUG ) {
            printf STDERR "[   Backend] File does not exist, return 404: $dest\n";
        }
        return $req->new_response(404, [], []);
    }
    unlink $dest;

    return $req->new_response(204, [], []);
}

1;