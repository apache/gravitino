/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

/**
 * An auditable entity is an entity that has audit information associated with it. This audit
 * information is used to track changes to the entity.
 */
public interface Auditable {

  /** @return The audit information of the entity. */
  Audit auditInfo();
}
