use strict;
use Plack::Builder;
use STF::Dispatcher;
use STF::Dispatcher::PSGI;

my $dispatcher = STF::Dispatcher->bootstrap;
my $context    = $dispatcher->context;
my $config     = $context->config;
my $app        = STF::Dispatcher::PSGI->new( impl => $dispatcher )->to_app;

builder {
    if ( my $basic_auth_db = $ENV {BASIC_AUTH_DB} ) {
        require DBI;
        my $connect_info = $config->{ $basic_auth_db };
        enable_if { $_[0]->{REQUEST_METHOD} !~ /^(?:GET|HEAD)$/i } 'Auth::Basic', 
            realm => 'STF',
            authenticator => sub {
                my ($username, $password) = @_;
                my $dbh = DBI->connect( @$connect_info );
                my $sth = $dbh->prepare(<<EOSQL);
                    SELECT password FROM client WHERE name = ?
EOSQL
                my $rv = $sth->execute($username);
                return unless $rv > 0;
                my ($crypted) = $sth->fetchrow_array();
                $sth->finish;
                return crypt( $password, $crypted ) eq $crypted;
            }
        ;
    }

    if ( $ENV{ PLACK_ENV } eq 'production' ) {
        # XXX Only enabling in production so that under dev,
        # people will be smart and not use urls with extraneous slashes
        enable sub {
            my $app = shift;
            sub { $_[0]->{PATH_INFO} =~ s!/+!/!g; $app->($_[0]) }
        };

        enable 'ReverseProxy';
        enable 'ServerStatus::Lite',
            path => '/___server-status',
            allow => [ qw(127.0.0.1 10.0.0.0/8) ],
            scoreboard => $ENV{ SCOREBOARD_DIR } ||
                -d "/var/run/stf" ? "/var/run/stf" : Cwd::cwd(),
        ;
    }
    $app
};
