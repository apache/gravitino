package org.apache.gravitino.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/** Exception thrown when an entity is in use and cannot be deleted. */
public class EntityInUseException extends GravitinoRuntimeException {
  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public EntityInUseException(@FormatString String message, Object... args) {
    super(message, args);
  }
}
