package com.datastrato.gravitino.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

public class NoSuchUserException extends NotFoundException {

  @FormatMethod
  public NoSuchUserException(@FormatString String message, Object... args) {
    super(message, args);
  }

  @FormatMethod
  public NoSuchUserException(Throwable cause, String message, Object... args) {
    super(cause, message, args);
  }
}
