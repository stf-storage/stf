package STF::CLI;
use strict;
use Getopt::Long ();
use Class::Load ();
use STF::Context;

sub new {
    my $class = shift;
    my $self = bless {
        cmds => {
            check     => 'STF::CLI::Check',
            crash     => 'STF::CLI::Crash',
            object    => 'STF::CLI::Object',
            redistribute => 'STF::CLI::Redistribute',
            repair    => 'STF::CLI::Repair',
            replicate => 'STF::CLI::Replicate',
            retire    => 'STF::CLI::Retire',
            status    => 'STF::CLI::Status',
            usage     => 'STF::CLI::Usage',
        }
    }, $class;
    $self;
}

sub base_opt_specs { ('config=s', 'debug') }

sub run {
    my $self = shift;
    my $base_opts = $self->get_options( $self->base_opt_specs );
    my $command = shift @ARGV;
    if (! $command) {
        $self->show_subcommands( "Missing subcommand" );
        exit 1;
    }

    my $class = $self->{cmds}->{lc $command};
    if (! $class) {
        $self->show_subcommands( "No such command" );
        exit 1;
    }

    Class::Load::load_class( $class )
        if ! Class::Load::is_class_loaded($class);

    my $opts = $self->get_options( $class->opt_specs );
    my %options = (
        %{$base_opts},
        %{$opts},
    );
    local $ENV{LM_DEBUG} = 1 if $options{debug};

    my $context = STF::Context->bootstrap;
    my $guard = $context->container->new_scope();
    my $c = $class->new( 
        context => $context,
        options => \%options,
    );
    $c->run( @ARGV );
}

sub get_options {
    my( $self, @specs ) = @_;
    my $p = Getopt::Long::Parser->new;
    $p->configure(qw(pass_through));
    if ($p->getoptions( \my %hash, @specs )) {
        return \%hash;
    } 
}

sub show_subcommands {
    my ($self, $message) = @_;
    print STDOUT "$message\n";
    print STDOUT <<EOM
    crash <storage_id>
    object --path=str
    object --storage=int [--limit=int]
    repair [--offset=int] [--limit=int] [--procs=int]
    replicate <object_id>
    retire <storage_id>
    orphan <storage id> <storage path>
    usage  [--update]

repair [-o object_id]
repair [-s storage_id] [-L] [-P]
    With -o, schedules to repair one object instance specified by <object_id>.

    With -s, runs checks on the entities in the given storage.
    -L specifies that it should check for *LOGICAL* repairs - that is to say,
    it will queue objects which seem to not have enough entities.

    -P specifies that it should check for *PHYSICAL* repairs - that is to say,
    it will queue objects which exist in entity table, but does not actually
    exist in the storage.

    By default -L will be performed.
    
EOM
}

1;

__END__

=head1 NAME

STF::CLI - STF CLI

=head1 SYNOPSIS


=cut
