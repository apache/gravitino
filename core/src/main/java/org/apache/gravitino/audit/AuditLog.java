package org.apache.gravitino.audit;

/** The interface define unified audit log schema. */
public interface AuditLog {
  /**
   * The user who do the operation.
   *
   * @return user name.
   */
  String user();

  /**
   * The operation name.
   *
   * @return operation name.
   */
  String operation();

  /**
   * The identifier of the resource.
   *
   * @return resource identifier name.
   */
  String identifier();

  /**
   * The timestamp of the operation.
   *
   * @return operation timestamp.
   */
  String timestamp();
}
