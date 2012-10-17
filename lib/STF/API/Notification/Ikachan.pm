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
    my @res = $furl->post( "$url/$method", [], [
        channel => $channel, message => $args->{message}
    ]);
use Data::Dumper::Concise;
warn Dumper(\@res);
}

1;