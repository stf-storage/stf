-- migrate from non-clustered stf storage to clustered.

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
