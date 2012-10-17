package STF::Worker::Notify;
use Mouse;
use STF::Constants qw(STF_DEBUG);
use STF::Log;
use STF::API::Notification::Pattern;

extends 'STF::Worker::Base';
with 'STF::Trait::WithDBI';

has '+loop_class' => (
    default => sub {
        $ENV{ STF_QUEUE_TYPE } || 'Q4M',
    }
);

has patterns => (
    is => 'rw',
    default => sub { +[] }
);

sub work_once {
    my ($self, $notification_id) = @_;

    local $STF::Log::PREFIX = "Notify";
    eval {
        $self->notify($notification_id);
    };
    if ($@) {
        Carp::confess("Failed to notify: $@");
    }
}

sub reload {
    my $self = shift;


    $self->patterns( [ STF::API::Notification::Pattern->new(
        notifier_name => "API::Notification::Ikachan",
        operation     => "eq",
        op_field      => "ntype",
        op_arg        => "hello.world",
        extra_args    => { channel => "#stf" },
    ) ] );
}

sub notify {
    my ($self, $notification_id) = @_;

    my $notification = $self->get('API::Notification')->lookup($notification_id);
    return unless $notification;

    foreach my $pattern ( @{$self->patterns} ) {
        next unless $pattern->match($notification);

        my $notifier = eval { $self->get($pattern->notifier_name) };
        if ($@) {
            critf("Error while trying to notify: %s", $@);
            next;
        }
        $notifier->notify($notification, $pattern->extra_args);
    }
}

no Mouse;

1;
