package STF::API::Notification::Pattern;
use Mouse;
use feature 'switch';

has notifier_name => (
    is => 'ro',
);

has extra_args => (
    is => 'ro',
);

has operation => (
    is => 'ro', # 'eq', 'ne', 'lt', 'gt', '==', '!=', '>=', '<=', '=~'
);

has op_field => (
    is => 'ro',
    required => 1,
);
has op_arg => (
    is => 'ro',
    required => 1,
);

sub match {
    my ($self, $args) = @_;
    my $match = do {
        given ($self->operation) {
            $args->{$self->op_field} eq $self->op_arg when ("eq");
            $args->{$self->op_field} == $self->op_arg when ("==");
            $args->{$self->op_field} != $self->op_arg when ("!=");
            $args->{$self->op_field} >= $self->op_arg when (">=");
            $args->{$self->op_field} <= $self->op_arg when ("<=");
            $args->{$self->op_field} =~ $self->op_arg when ("=~");
        }
    };

    return $match ? 1 :();
}

1;
