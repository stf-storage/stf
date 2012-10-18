package STF::Worker::Notify;
use Mouse;
use STF::Constants qw(STF_DEBUG);
use STF::Log;
use STF::API::NotificationRule;

extends 'STF::Worker::Base';
with 'STF::Trait::WithDBI';

has '+loop_class' => (
    default => sub {
        $ENV{ STF_QUEUE_TYPE } || 'Q4M',
    }
);

has rules => (
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
    my @rules = $self->get("API::NotificationRule")->search(
        { status => 1, },
    );
    $self->rules([ map { STF::API::NotificationRule::Matcher->new(%$_) } @rules ]);
}

sub notify {
    my ($self, $notification_id) = @_;

    my $notification = $self->get('API::Notification')->lookup($notification_id);
    return unless $notification;

    foreach my $rule ( @{$self->rules} ) {
        next unless $rule->match($notification);

        my $notifier = eval { $self->get($rule->notifier_name) };
        if ($@) {
            critf("Error while trying to notify: %s", $@);
            next;
        }
        my $extra_args = $self->get('JSON')->decode($rule->extra_args || "null");
        $notifier->notify($notification, $extra_args);
    }
}

no Mouse;

1;
