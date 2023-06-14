package com.datastrato.graviton.meta;

public interface Auditable {

  /**
   * Returns the audit information of the entity.
   *
   * @return AuditInfo
   */
  AuditInfo auditInfo();
}
