/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.utils;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import java.sql.SQLIntegrityConstraintViolationException;

public class ExceptionUtils {
  private ExceptionUtils() {}

  public static void checkSQLConstraintException(
      RuntimeException re, Entity.EntityType type, String entityName) {
    if (re.getCause() != null
        && re.getCause() instanceof SQLIntegrityConstraintViolationException) {
      // TODO We should make more fine-grained exception judgments
      //  Usually throwing `SQLIntegrityConstraintViolationException` means that
      //  SQL violates the constraints of `primary key` and `unique key`.
      //  We simply think that the entity already exists at this time.
      throw new EntityAlreadyExistsException(
          String.format("%s entity: %s already exists", type.name(), entityName));
    }
  }
}
