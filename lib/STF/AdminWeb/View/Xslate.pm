package STF::AdminWeb::View::Xslate;
use Mouse;

use Encode ();
use Text::Xslate;
use HTML::FillInForm::Lite;

has fif => (
    is => 'rw',
    default => sub {
        HTML::FillInForm::Lite->new;
    }
);

has suffix => (
    is => 'rw',
);

has xslate => (
    is => 'rw', 
    required => 1,
);

sub BUILDARGS {
    my ($class, %args) = @_;

    my $app = delete $args{app};
    my $function = $args{function} ||= {};
    $function->{nl2br} = Text::Xslate::html_builder(sub {
        my $text = "$_[0]";
        $text =~ s{\n}{<br />}gsm;
        return $text;
    });
    $function->{loc} = sub {
        $app->context->get('Localizer')->localize(@_);
    };
    $function->{strftime} = sub { POSIX::strftime($_[0], localtime($_[1])) };

    my %parsed;
    if (my $fif = delete $args{fif}) {
        $parsed{fif} = $fif;
    }
    if (my $suffix = delete $args{suffix}) {
        $parsed{suffix} = $suffix;
    }
    $parsed{xslate} = Text::Xslate->new(%args);

    return \%parsed;
}

sub process {
    my ($self, $context, $template) = @_;

    my $content  = $self->render( $template, $context->stash );
    my $response = $context->response;
    my $request  = $context->request;

    $response->content_type( "text/html" );

    if ($response->content_type && $response->content_type =~ m{^text/x?html$}i) {
        if ( $request->method eq 'POST' ) {
            $content = $self->fif->fill( \$content, $request );
        } elsif ( my $fdat = $context->stash->{fdat} ) {
            $content = $self->fif->fill( \$content, $fdat );
        }
    }

    $response->body( Encode::encode_utf8( $content ) );
}

sub render {
    my ($self, $template, $vars) = @_;

    if (my $suffix = $self->suffix) {
        $template =~ s/(?<!$suffix)$/$suffix/;
    }

    $self->xslate->render( $template, $vars );
}

no Mouse;

1;
