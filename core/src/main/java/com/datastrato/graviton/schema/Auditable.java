package com.datastrato.graviton.schema;

public interface Auditable {

  /**
   * Returns the audit information of the entity.
   *
   * @return AuditInfo
   */
  AuditInfo auditInfo();
}
