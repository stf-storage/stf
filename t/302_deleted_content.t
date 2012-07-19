use strict;
use Test::More;
use Plack::Test;
use HTTP::Request::Common qw(PUT HEAD GET POST);
use HTTP::Date;
use Scope::Guard ();
use STF::Test;
use STF::Test qw(clear_queue);
BEGIN {
    use_ok "STF::Constants",
        "STF_TRACE",
        "STORAGE_MODE_READ_WRITE",
        "STORAGE_MODE_TEMPORARILY_DOWN",
    ;
}

use_ok "STF::Context";
use_ok "STF::Worker::Replicate";
use_ok "STF::Worker::RepairObject";

# String::URandomとか使っても良いけど面倒くさい
my $random_string = sub {
    my @chars = ('a'..'z');
    join "", map { $chars[ rand @chars ] } 1..($_[0] || 8);
};

# PUT an object (success)
# GET an object (success)
# Storage->update($id, mode => STORAGE_MODE_TEMPORARILY_DOWN );
# GET an object, and this should succeed, FAST
my $code = sub {
    my ($cb) = @_;
    my $res;
    my $bucket_name = $random_string->();
    my $object_name = $random_string->();

    $res = $cb->(
        PUT "http://127.0.0.1/$bucket_name"
    );
    if (! ok $res->is_success, "bucket creation request was successful") {
        diag $res->as_string;
    }

    $res = $cb->(
        PUT "http://127.0.0.1/$bucket_name/$object_name",
            "X-STF-Consistency" => 3,
            "Content" => $random_string->(1024)
    );

    my $context = STF::Context->bootstrap( config => "t/config.pl" ) ;
    # XXX Find where this object belongs to
    my $container = $context->container;
    {
        my $guard = $container->new_scope();
        my $worker = STF::Worker::Replicate->new(
            container => $container,
            max_works_per_child => 1,
        );
        $worker->work;
    }

    for (1..5) {
        $res = $cb->(
            GET "http://127.0.0.1/$bucket_name/$object_name",
        );
        if (! ok $res->is_success, "GET is successful") {
            diag $res->as_string;
        }
    }

    my $dbh = $container->get('DB::Master');
    my $object = $dbh->selectrow_hashref( <<EOSQL, undef, $bucket_name, $object_name );
        SELECT o.* FROM object o
            JOIN bucket b ON o.bucket_id = b.id
            WHERE b.name = ? AND o.name = ?
EOSQL
    ok $object;

    # make sure we have >= 3 entities in filesystem
    my @physical_entities = glob("t/store*/$object->{internal_name}");
    if (! ok scalar @physical_entities >= 3, "3 entities available") {
        diag explain @physical_entities;
    }

    my $deleted = $physical_entities[rand scalar @physical_entities];
    unlink $deleted;

    # there's no mapping from physical path to storage, so just
    # pump all the entities through the worker to fix it
    my @entities = $container->get('API::Entity')->search(
        {
            object_id => $object->{id},
        },
        {
            order_by => 'rand()'
        }
    );

    my $queue_api = $container->get('API::Queue');
    foreach my $entity (@entities) {
        $queue_api->enqueue( repair_object => "$entity->{object_id}:$entity->{storage_id}" );
    }

    {
        eval {
            local $SIG{ALRM} = sub { die "RepairObject timeout" };
            alarm(5);
            my $worker = STF::Worker::RepairObject->new(
                container => $context->container,
                max_works_per_child => scalar @entities,
            );
            $worker->work;
        };
        if ($@) {
            fail "Error running RepairObject worker: $@";
        }
        alarm(0);
    }

    ok -f $deleted, "File was replicated";

    for (1..5) {
        $res = $cb->(
            GET "http://127.0.0.1/$bucket_name/$object_name",
        );
        if (! ok $res->is_success, "GET is successful") {
            diag $res->as_string;
        }
    }
};

clear_queue();
my $app = require "t/dispatcher.psgi";
test_psgi
    app => $app,
    client => $code,
;

done_testing;