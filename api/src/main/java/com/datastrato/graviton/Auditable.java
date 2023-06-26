package com.datastrato.graviton;

public interface Auditable {

  /** Returns the audit information of the entity. */
  Audit auditInfo();
}
