package STF::CLI;
use strict;
use Getopt::Long ();
use Class::Load ();
use STF::Context;

sub new {
    my $class = shift;
    my $self = bless {
        cmds => {
            enqueue   => 'STF::CLI::Enqueue',
            health    => 'STF::CLI::Health',
            object    => 'STF::CLI::Object',
            storage   => 'STF::CLI::Storage',
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

    if (! Class::Load::try_load_class($class) ) {
        print STDERR "Could not $class\n";
        exit 1;
    }
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

$0 <subcommand> [options...]

health [-a] <id-is>
health [-a] -s <storage-id> -l <limit>

    Displays the health status of an object. This means actual HTTP requests
    will run to check if the entities are actuall retrievable

object <id-ish>
object -s <storage-id> -l <limit>

    Displays the object details. <id-ish> can be an object path or object ID.

    -l <storage-id> will show objects in the storage

storage <id>
storage -L 
    Displays the storage status.

    -l will show the entire storage list.

enqueue <job-name> <arg>

    Enqueues the piece of into the <job-name> queue. Job name may be:
    replicate, delete_bucket, delete_object, repair_object, object_health
    
EOM
}

1;

__END__

=head1 NAME

STF::CLI - STF CLI

=head1 SYNOPSIS


=cut
