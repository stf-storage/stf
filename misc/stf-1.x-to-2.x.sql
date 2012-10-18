-- migrate from non-clustered stf storage to clustered.

CREATE TABLE config (
    varname VARCHAR(127) NOT NULL PRIMARY KEY,
    varvalue TEXT
) ENGINE=InnoDB;

REPLACE INTO config (varname, varvalue)
    VALUES ("stf.drone.AdaptiveThrottler.instances", 1);
REPLACE INTO config (varname, varvalue)
    VALUES ("stf.drone.Replicate.instances", 8);
REPLACE INTO config (varname, varvalue)
    VALUES ("stf.drone.RepairObject.instances", 4);
REPLACE INTO config (varname, varvalue)
    VALUES ("stf.drone.DeleteBucket.instances", 2);
REPLACE INTO config (varname, varvalue)
    VALUES ("stf.drone.DeleteObject.instances", 2);
REPLACE INTO config (varname, varvalue)
    VALUES ("stf.drone.RepairStorage.instances", 1);
REPLACE INTO config (varname, varvalue)
    VALUES ("stf.drone.ContinuousRepair.instances", 1);
REPLACE INTO config (varname, varvalue)
    VALUES ("stf.drone.StorageHealth.instances", 1);
REPLACE INTO config (varname, varvalue)
    VALUES ("stf.drone.Notify.instances", 1);

REPLACE INTO config (varname, varvalue)
    VALUES ("stf.worker.RepairObject.throttle.auto_adapt", 1);
REPLACE INTO config (varname, varvalue)
    VALUES ("stf.worker.RepairObject.throttle.threshold", 300);
REPLACE INTO config (varname, varvalue)
    VALUES ("stf.worker.RepairObject.throttle.current_threshold", 0);

CREATE TABLE storage_cluster (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(128),
    mode TINYINT NOT NULL DEFAULT 1,
    KEY (mode)
) ENGINE=InnoDB;

ALTER TABLE storage ADD COLUMN cluster_id INT;
ALTER TABLE storage ADD FOREIGN KEY (cluster_id) REFERENCES storage_cluster (id) ON DELETE SET NULL;

CREATE TABLE object_cluster_map (
    object_id BIGINT NOT NULL,
    cluster_id INT NOT NULL,
    PRIMARY KEY(object_id),
    FOREIGN KEY (object_id) REFERENCES object (id) ON DELETE CASCADE,
    FOREIGN KEY (cluster_id) REFERENCES storage_cluster (id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE worker_election (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    drone_id VARCHAR(255) NOT NULL,
    expires_at INT NOT NULL,
    UNIQUE KEY (drone_id),
    KEY (expires_at)
) ENGINE=InnoDB DEFAULT CHARACTER SET = 'utf8';

CREATE TABLE worker_instances (
    drone_id VARCHAR(255) NOT NULL,
    worker_type VARCHAR(255) NOT NULL,
    instances INT NOT NULL DEFAULT 1,
    PRIMARY KEY(drone_id, worker_type),
    FOREIGN KEY(drone_id) REFERENCES worker_election (drone_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET = 'utf8';

CREATE TABLE notification (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ntype CHAR(40) NOT NULL,
    /* source of this notification. should include file + linu num */
    source TEXT NOT NULL,
    /* severity of this notification: critical, info ? anything else? */
    severity VARCHAR(32) NOT NULL DEFAULT 'info',
    message TEXT NOT NULL,
    created_at INT NOT NULL,
    KEY(created_at),
    KEY(ntype)
) ENGINE=InnoDB DEFAULT CHARACTER SET = 'utf8';

CREATE TABLE notification_rule (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    /* status: 0 -> suspended, won't execute. status: 1 -> will execute */
    status TINYINT NOT NULL DEFAULT 1,

    /* operation type, "eq", "ne", "==", "!=", "<=", ">=", "=~" */
    operation CHAR(2) NOT NULL,
    /* which notificiation object field to apply thie operation against */
    op_field  VARCHAR(255) NOT NULL DEFAULT "ntype",
    /* user-defined operand.
       e.g., op_field = "ntype", operation = "=~", op_arg = "^foo"
             yields "ntype" =~ /^foo/
    */
    op_arg    VARCHAR(255) NOT NULL,
    /* user-defined extra set of arguments that are required by
       the notifier to complete the notification. e.g.
       Ikachan notification requires "channel", email notification
       requires "to" address.
       encoded as JSON string
    */
    extra_args TEXT,
    /* which notifier to invoke upon rule match. Must be able to
       look this up via container->get. e.g. API::Notification::Email
    */
    notifier_name VARCHAR(255) NOT NULL,
    KEY(status)
) ENGINE=InnoDB DEFAULT CHARACTER SET = 'utf8';


