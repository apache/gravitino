select catalog_name, status, failure_count from gravitino.system.catalog_status where catalog_name = 'gt_mysql';

select cast(trino_started as varchar), consecutive_failures from gravitino.system.load_status;
