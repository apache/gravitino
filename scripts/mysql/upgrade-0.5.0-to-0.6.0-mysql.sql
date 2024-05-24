--
-- Copyright 2024 Datastrato Pvt Ltd.
-- This software is licensed under the Apache License version 2.
--

-- <issue-3099: Store role, user, and group under {metalake}.system.{role|user|group} in relation storage>
ALTER TABLE `user_meta`
    ADD COLUMN `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id' AFTER `metalake_id`;
ALTER TABLE `user_meta`
    ADD COLUMN `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id' AFTER `catalog_id`;

ALTER TABLE `group_meta`
    ADD COLUMN `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id' AFTER `metalake_id`;
ALTER TABLE `group_meta`
    ADD COLUMN `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id' AFTER `catalog_id`;

ALTER TABLE `role_meta`
    ADD COLUMN `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id' AFTER `metalake_id`;
ALTER TABLE `role_meta`
    ADD COLUMN `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id' AFTER `catalog_id`;

-- This stored procedure is designed to initialize system catalog and schema for each metalake
-- and it updates the `catalog_id` and `schema_id` in the `user_meta`, `group_meta`, and `role_meta` tables to the newly generated `catalog_id` and `schema_id`.
DELIMITER $$

CREATE PROCEDURE InitializeSystemCatalogAndSchema()
BEGIN
    DECLARE v_metalake_id BIGINT(20) UNSIGNED;
    DECLARE v_catalog_id BIGINT(20) UNSIGNED;
    DECLARE v_schema_id_user BIGINT(20) UNSIGNED;
    DECLARE v_schema_id_group BIGINT(20) UNSIGNED;
    DECLARE v_schema_id_role BIGINT(20) UNSIGNED;
    DECLARE v_now VARCHAR(255);
    DECLARE done INT DEFAULT FALSE;

    DECLARE cur CURSOR FOR SELECT metalake_id FROM metalake_meta WHERE deleted_at = 0;
    -- Declare a continue handler for when no more rows are found in the cursor
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    -- Declare an exit handler for any SQL exception that will rollback the transaction
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT 'An error occurred, rolling back transaction' AS message;
        ROLLBACK;
    END;

    SET v_now = DATE_FORMAT(NOW(), '%Y-%m-%dT%H:%i:%s.000Z');

    START TRANSACTION;

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO v_metalake_id;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- 9223372036854775807 is the maximum number that RandomIdGenerator can generate
        SET v_catalog_id = FLOOR(RAND() * 9223372036854775807);
        SET @audit_info = CONCAT('{"creator":"system","createTime":"', v_now, '","lastModifier":null,"lastModifiedTime":null}');

        INSERT INTO catalog_meta (catalog_id, metalake_id, catalog_name, type, provider, catalog_comment, properties, audit_info, current_version, last_version, deleted_at)
        VALUES (v_catalog_id, v_metalake_id, 'system', 'system', 'system', 'reserved system catalog', NULL, @audit_info, 1, 1, 0);

        SET v_schema_id_user = FLOOR(RAND() * 9223372036854775807);
        INSERT INTO schema_meta (schema_id, metalake_id, catalog_id, schema_name, schema_comment, properties, audit_info, current_version, last_version, deleted_at)
        VALUES (v_schema_id_user, v_metalake_id, v_catalog_id, 'user', 'reserved user schema', NULL, @audit_info, 1, 1, 0);

        SET v_schema_id_group = FLOOR(RAND() * 9223372036854775807);
        INSERT INTO schema_meta (schema_id, metalake_id, catalog_id, schema_name, schema_comment, properties, audit_info, current_version, last_version, deleted_at)
        VALUES (v_schema_id_group, v_metalake_id, v_catalog_id, 'group', 'reserved group schema', NULL, @audit_info, 1, 1, 0);

        SET v_schema_id_role = FLOOR(RAND() * 9223372036854775807);
        INSERT INTO schema_meta (schema_id, metalake_id, catalog_id, schema_name, schema_comment, properties, audit_info, current_version, last_version, deleted_at)
        VALUES (v_schema_id_role, v_metalake_id, v_catalog_id, 'role', 'reserved role schema', NULL, @audit_info, 1, 1, 0);

        UPDATE user_meta SET catalog_id = v_catalog_id, schema_id = v_schema_id_user WHERE metalake_id = v_metalake_id;
        UPDATE group_meta SET catalog_id = v_catalog_id, schema_id = v_schema_id_group WHERE metalake_id = v_metalake_id;
        UPDATE role_meta SET catalog_id = v_catalog_id, schema_id = v_schema_id_role WHERE metalake_id = v_metalake_id;

    END LOOP;

    -- Close the cursor
    CLOSE cur;

    -- Commit the transaction
    COMMIT;
END$$

DELIMITER ;

call InitializeSystemCatalogAndSchema();
