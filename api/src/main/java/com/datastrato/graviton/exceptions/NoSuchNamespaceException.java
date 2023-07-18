/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton.exceptions;

/** Exception thrown when a namespace with specified name is not existed. */
public class NoSuchNamespaceException extends NotFoundException {

  public NoSuchNamespaceException(String message) {
    super(message);
  }

  public NoSuchNamespaceException(String message, Throwable cause) {
    super(message, cause);
  }
}
