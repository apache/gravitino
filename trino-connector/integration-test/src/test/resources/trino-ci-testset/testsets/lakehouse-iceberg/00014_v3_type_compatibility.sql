CREATE SCHEMA gt_v3_type_compatibility;

USE gt_v3_type_compatibility;

CREATE TABLE rejected_nanosecond_timestamp (
    value TIMESTAMP(9)
);

SHOW TABLES LIKE 'rejected_nanosecond_timestamp';

CREATE TABLE rejected_geometry (
    value GEOMETRY
);

SHOW TABLES LIKE 'rejected_geometry';

CREATE TABLE rejected_geography (
    value SPHERICALGEOGRAPHY
);

SHOW TABLES LIKE 'rejected_geography';

DROP SCHEMA gt_v3_type_compatibility;
