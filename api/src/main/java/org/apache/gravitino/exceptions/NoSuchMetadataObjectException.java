package org.apache.gravitino.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/** Exception thrown when a metadata object with specified name is not existed. */
public class NoSuchMetadataObjectException extends NotFoundException {
  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public NoSuchMetadataObjectException(@FormatString String message, Object... args) {
    super(message, args);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param cause the cause.
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public NoSuchMetadataObjectException(Throwable cause, String message, Object... args) {
    super(cause, message, args);
  }
}
