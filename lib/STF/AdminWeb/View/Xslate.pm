package STF::AdminWeb::View::Xslate;
use strict;
use Encode ();
use Text::Xslate;
use Class::Accessor::Lite
    rw => [ qw(
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
        suffix => delete $args{suffix},
        xslate => Text::Xslate->new(%args),
    }, $class;
}

sub process {
    my ($self, $context, $template) = @_;

    my $content = $self->render( $template, $context->stash );
    my $response = $context->response;
    $response->content_type( "text/html" );
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
