package STF::API::Notification;
use Mouse;

with 'STF::API::WithDBI';

around create => sub {
    my ($next, $self, $args) = @_;
    return unless $self->$next($args);
    my $object = $self->lookup($self->dbh->{mysql_insertid});
    $self->get('API::Queue')->enqueue(notify => $object->{id});
    return $object;
};

no Mouse;

1;