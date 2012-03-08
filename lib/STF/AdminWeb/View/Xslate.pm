package STF::AdminWeb::View::Xslate;
use strict;
use Encode ();
use Text::Xslate;
use HTML::FillInForm::Lite;
use Class::Accessor::Lite
    rw => [ qw(
        fif
        suffix
        xslate
    ) ]
;

sub new {
    my ($class, %args) = @_;
    my $app = delete $args{app};

    my $function = $args{function} ||= {};
    $function->{nl2br} = Text::Xslate::html_builder(sub {
        my $text = "$_[0]";
        $text =~ s{\n}{<br />}gsm;
        return $text;
    });

    bless {
        fif    => HTML::FillInForm::Lite->new,
        suffix => delete $args{suffix},
        xslate => Text::Xslate->new(%args),
    }, $class;
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

1;
