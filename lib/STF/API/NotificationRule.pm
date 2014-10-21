package STF::API::NotificationRule;
use Mouse;

with 'STF::API::WithDBI';

package
    STF::API::NotificationRule::Matcher;
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

    # XXX CI smoking shows that perl 5.14.x and 5.12.x behave differently
    # in terms of return value for give() {...} block. 5.12.x seems to
    # NOT return the value of the evaluated when block. So explicitly
    # assign to $match in each when()
    my $match = 0;
    my $op = $self->operation;
    if ($op eq "eq") {
        $match = $args->{$self->op_field} eq $self->op_arg;
    } elsif ($op eq "==") {
        $match = $args->{$self->op_field} == $self->op_arg;
    } elsif ($op eq "!=") {
        $match = $args->{$self->op_field} != $self->op_arg;
    } elsif ($op eq ">=") {
        $match = $args->{$self->op_field} >= $self->op_arg;
    } elsif ($op eq "<=") {
        $match = $args->{$self->op_field} <= $self->op_arg;
    } elsif ($op eq "=~") {
        $match = $args->{$self->op_field} =~ $self->op_arg;
    }

    return $match ? 1 :();
}

no Mouse;

1;
