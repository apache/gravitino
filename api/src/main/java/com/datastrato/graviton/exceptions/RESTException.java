package com.datastrato.graviton.exceptions;

import com.google.errorprone.annotations.FormatMethod;

public class RESTException extends RuntimeException {
  @FormatMethod
  public RESTException(String message, Object... args) {
    super(String.format(message, args));
  }

  @FormatMethod
  public RESTException(Throwable cause, String message, Object... args) {
    super(String.format(message, args), cause);
  }
}
