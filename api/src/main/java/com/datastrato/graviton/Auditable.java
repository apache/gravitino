/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

public interface Auditable {

  /** Returns the audit information of the entity. */
  Audit auditInfo();
}
