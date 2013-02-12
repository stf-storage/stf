package STF::AdminWeb::Controller::Worker;
use Mojo::Base 'STF::AdminWeb::Controller';

sub api_list {
    my $self = shift;

    # Find worker instances
    my @workers = $self->get("API::WorkerInstances")->search();
    $self->render_json({
        workers => \@workers
    });
}

1;