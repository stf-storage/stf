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

CREATE TABLE storage (
       id INT NOT NULL PRIMARY KEY,
       cluster_id INT,
       uri VARCHAR(100) NOT NULL,
       mode TINYINT NOT NULL DEFAULT 1,
       created_at INT NOT NULL,
       updated_at TIMESTAMP,
       FOREIGN KEY(cluster_id) REFERENCES storage_cluster (id) ON DELETE SET NULL,
       UNIQUE KEY(uri),
       KEY(mode)
) ENGINE=InnoDB;

/*
    storage_meta - Used to store storage meta data.

    This is a spearate table because historically the 'storage' table
    was declared without a character set declaration, and things go
    badly when multibyte 'notes' are added.

    Make sure to place ONLY items that has nothing to do with the
    core STF functionality here.

    XXX Theoretically this table could be in a different database
    than the main mysql instance.
*/
CREATE TABLE storage_meta (
    storage_id INT NOT NULL PRIMARY KEY,
    used       BIGINT UNSIGNED DEFAULT 0,
    capacity   BIGINT UNSIGNED DEFAULT 0,
    notes      TEXT,
    /* XXX if we move this table to a different database, then
       this foreign key is moot. this is placed where because I'm 
       too lazy to cleanup the database when we delete the storage
    */
    FOREIGN KEY(storage_id) REFERENCES storage(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE bucket (
       id BIGINT NOT NULL PRIMARY KEY,
       name VARCHAR(255) NOT NULL,
       objects BIGINT UNSIGNED NOT NULL DEFAULT 0,
       created_at INT NOT NULL,
       updated_at TIMESTAMP,
       UNIQUE KEY(name)
) ENGINE=InnoDB;

CREATE TABLE object (
       id BIGINT NOT NULL PRIMARY KEY,
       bucket_id BIGINT NOT NULL,
       name VARCHAR(255) NOT NULL,
       internal_name VARCHAR(128) NOT NULL,
       size INT NOT NULL DEFAULT 0,
       num_replica INT NOT NULL DEFAULT 1,
       status TINYINT DEFAULT 1 NOT NULL,
       created_at INT NOT NULL,
       updated_at TIMESTAMP,
       UNIQUE KEY(bucket_id, name),
       UNIQUE KEY(internal_name)
) ENGINE=InnoDB;

/* object_cluster_map 
    maps objects to clusters
*/
CREATE TABLE object_cluster_map (
    object_id BIGINT NOT NULL,
    cluster_id INT NOT NULL,
    PRIMARY KEY(object_id),
    FOREIGN KEY (object_id) REFERENCES object (id) ON DELETE CASCADE,
    FOREIGN KEY (cluster_id) REFERENCES storage_cluster (id) ON DELETE CASCADE
) ENGINE=InnoDB;

/* object_meta - same caveats as storage_meta applies */
CREATE TABLE object_meta (
    object_id BIGINT NOT NULL PRIMARY KEY,
    hash      CHAR(32),
    FOREIGN KEY(object_id) REFERENCES object(id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE deleted_object ENGINE=InnoDB SELECT * FROM object LIMIT 0;
ALTER TABLE deleted_object ADD PRIMARY KEY(id);
-- ALTER TABLE deleted_object ADD UNIQUE KEY(internal_name);
CREATE TABLE deleted_bucket ENGINE=InnoDB SELECT * FROM bucket LIMIT 0;
ALTER TABLE deleted_bucket ADD PRIMARY KEY(id);

CREATE TABLE entity (
       object_id BIGINT NOT NULL,
       storage_id INT NOT NULL,
       status TINYINT DEFAULT 1 NOT NULL,
       created_at INT NOT NULL,
       updated_at TIMESTAMP,
       PRIMARY KEY id (object_id, storage_id),
       KEY(object_id, status),
       KEY(storage_id),
       FOREIGN KEY(storage_id) REFERENCES storage(id) ON DELETE RESTRICT
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

