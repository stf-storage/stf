use strict;
use Test::More;
use_ok "STF::Context";

my $context = STF::Context->bootstrap();
my $api = $context->get( 'API::Entity' );
ok($api);

# subtest 'replicate' => sub {
#     # Grab a random object
#     $api->replicate({
#         object_id => 
#         replicas  =>
#     } );
# };

done_testing;