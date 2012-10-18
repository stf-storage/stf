package STF::API::Notification::Ikachan;
use Mouse;
use STF::Constants qw(STF_DEBUG);
use STF::Log;

with 'STF::Trait::WithContainer';

has url => (
    is => 'rw',
    required => 1,
);

has method => (
    is => 'rw',
    default => 'notice'
);

has channel => (
    is => 'rw',
);

sub notify {
    my ($self, $args, $extra_args) = @_;

    my $method = $extra_args->{method} || $args->{method} || $self->method;
    my $channel = $extra_args->{channel} || $args->{channel} || $self->channel;
    if (! $method) {
        if (STF_DEBUG) {
            debugf("No channel specified for Notification::Ikachan, bailing out");
        }
        return;
    }

    my $url = $self->url;
    my $furl = $self->get('Furl');

    # do a join to make sure that we're in this channel (throw away results
    # -- we don't care)
    $furl->post( "$url/join", [], [ channel => $channel ]);

    my $message = $args->{message};
    if (my $severity = $args->{severity}) {
        $message = "[$severity] $message";
    }
    my ($code) = $furl->post( "$url/$method", [], [
        channel => $channel, message => $message
    ]);
    if (STF_DEBUG) {
        if ($code ne 200) {
            debugf("HTTP request to Ikachan seems to have returned %d", $code);
        }
    }
}

1;