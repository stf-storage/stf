# HATE HATE HATE HATE Data::FormValidator. It's a piece of shit code,
# with hacks that make it virtually impossible to extend it in a sane way.
#
# This class needs to add a whopping 76 lines *JUST* to add 1 attribute
# to the object.
#
# HATE HATE HATE Data::FormValidator. It's a PIECE OF SHIT.

package STF::DFV;
use strict;
use parent qw(Data::FormValidator);
use Class::Accessor::Lite
    rw => [ qw( container) ]
;

sub new {
    my $class = shift;
    my $profiles = shift;
    my $defaults = (ref $_[0] eq 'HASH') ? shift : {};
    my %args = @_;

    if (! exists $defaults->{missing_optional_valid} ) {
        $defaults->{missing_optional_valid} = 1;
    }
    $defaults->{msgs} = sub {
        my $dfv = shift;
        my %msgs;

        if ( $dfv->has_missing ) {
            foreach my $missing ($dfv->missing) {
                my $list = $msgs{ $missing } ||= [];
                push @$list, "error.missing";
            }
        }

        if ( $dfv->has_invalid ) {
            foreach my $invalid ($dfv->invalid) {
                my $failed = $dfv->invalid($invalid);
                my $list = $msgs{ $invalid } ||= [];
                push @$list, map {
                    ref $_          ? 'error.invalid' :
                    $_ eq 'eq_with' ? 'error.eq_with' : $_
                } @$failed;
            }
        }
        return \%msgs;
    };
        
    my $self = $class->SUPER::new( $profiles, $defaults );
    while ( my($field, $value) = each %args ) {
        $self->$field( $value );
    }
    return $self;
}


sub get { shift->container->get(@_) }

sub check {
    my ( $self, $data, $name ) = @_;

    # check can be used as a class method for simple cases
    if (not ref $self) {
        my $class = $self;
        $self = {};
        bless $self, $class;
    }

    my $profile;
    if ( ref $name ) {
        $profile = $name;
    } else {
        $self->load_profiles;
        $profile = $self->{profiles}{$name};
        die "No such profile $name\n" unless $profile;
    }
    die "input profile must be a hash ref" unless ref $profile eq "HASH";

    # add in defaults from new(), if any
    if ($self->{defaults}) {
        $profile = { %{$self->{defaults}}, %$profile };
    }

    # check the profile syntax or die with an error.
    Data::FormValidator::_check_profile_syntax($profile);

    my $results = STF::DFV::Results->new( $profile, $data, $self->container );

    # As a special case, pass through any defaults for the 'msgs' key.
    $results->msgs($self->{defaults}->{msgs}) if $self->{defaults}->{msgs};

    return $results;
}

package STF::DFV::Results;
use strict;
use parent qw(Data::FormValidator::Results);
use Class::Accessor::Lite
    rw => [ qw( container ) ]
;

sub new {
    my $proto = shift;
    my $class = ref $proto || $proto;
    my ($profile, $data, $container) = @_;

    my $self = bless { container => $container }, $class;

    $self->_process( $profile, $data );
    $self;
}

sub add_invalid {
    my ($self, $key, $error) = @_;
    my $list = $self->invalid->{$key} ||= [];
    push @$list, $error;
}

1;