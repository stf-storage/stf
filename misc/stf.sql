
CREATE TABLE storage (
       id INT NOT NULL PRIMARY KEY,
       uri VARCHAR(100) NOT NULL,
       mode TINYINT NOT NULL DEFAULT 1,
       used BIGINT  UNSIGNED NOT NULL DEFAULT 0,
       capacity BIGINT UNSIGNED NOT NULL DEFAULT 0,
       created_at INT NOT NULL,
       updated_at TIMESTAMP,
       UNIQUE KEY(uri),
       KEY(mode)
) ENGINE=InnoDB;

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

