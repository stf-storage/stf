package STF::API::Notification::Email;
use Mouse;
use STF::Log;
use STF::Constants qw(STF_DEBUG);
use Email::MIME;
use Email::Send;

has from => (
    is => 'ro',
    required => 1,
);

has mailer_args => (
    is => 'ro',
    default => sub { +{} }
);

has mailer_type => (
    is => 'ro',
    required => 1,
    default => 'Sendmail'
);

has mailer => (
    is => 'ro',
    lazy => 1,
    builder => 'build_mailer',
);

sub build_mailer {
    my $self = shift;
    my $mailer = Email::Send->new({ $self->mailer_type });
    if (my $args = $self->mailer_args) {
        $mailer->mailer_args($args);
    }
    return $mailer;
}

sub notify {
    my ($self, $args, $extra_args) = @_;

    my $to = $args->{to} || $extra_args->{to} || $self->to;
    my $message = $args->{message} || $extra_args->{message};
    # XXX Assume all latin-1 ?
    my $mime = Email::MIME->create(
        header => [
            From => $self->from,
            To   => $to,
            Subject => sprintf 'STF Notification [%s]', $args->{ntype},
        ],
        parts => [ $message ]
    );
    $self->mailer->send($mime);
    if (STF_DEBUG) {
        debugf("Email notification for %s has been sent to %s", $args->{ntype}, $to);
    }
}

no Mouse;

1;
