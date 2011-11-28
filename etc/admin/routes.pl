use Router::Simple::Declare;

router {
    connect '/' => {
        controller => 'Storage',
        action     => 'list',
    };

    connect qr{^/storage(?:/(?:list)?)?$} => {
        controller => 'Storage',
        action     => 'list',
    };

    connect '/storage/add' => {
        controller => 'Storage',
        action     => 'add',
    }, { method => 'GET' };
    connect '/storage/add' => {
        controller => 'Storage',
        action     => 'add_post',
    }, { method => 'POST' };

    foreach my $action (qw(edit)) {
        connect "/storage/:storage_id/$action" => {
            controller => 'Storage',
            action     => $action,
        }, { method => 'GET' };
        connect "/storage/:storage_id/$action" => {
            controller => 'Storage',
            action     => "${action}_post",
        }, { method => 'POST' };
    }

    foreach my $action (qw(entities)) {
        connect "/storage/:storage_id/$action" => {
            controller => 'Storage',
            action     => $action,
        };
    }

    connect '/storage/:storage_id/delete' => {
        controller => 'Storage',
        action     => 'delete_post',
    }, { method => 'POST' };

    connect qr{^/bucket(?:/(?:list)?)?$} => {
        controller => 'Bucket',
        action     => 'list',
    };

    connect '/bucket/:bucket_id' => {
        controller => 'Bucket',
        action     => 'view',
    };

    connect '/ajax/num_objects/:bucket_id' => {
        controller => 'Bucket',
        action     => 'objects',
    };

    connect qr{^/object/([^/]+)(/[\w\/%+._-]+)$} => {
        controller => 'Object',
        action => 'view_public_name',
    };

    connect '/object/:object_id' => {
        controller => 'Object',
        action     => 'view',
    };
};
