package com.datastrato.graviton;

public interface Auditable {

  /**
   * Returns the audit information of the entity.
   *
   * @return Audit
   */
  Audit auditInfo();
}
