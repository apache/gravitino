/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.exceptions;

import com.google.common.collect.Lists;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.List;

/** Exception thrown when a user is not authorized to perform an action. */
public class UnauthorizedException extends GravitinoRuntimeException {

  /** The challenges of the exception. */
  private final List<String> challenges = Lists.newArrayList();

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public UnauthorizedException(@FormatString String message, Object... args) {
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
  public UnauthorizedException(Throwable cause, @FormatString String message, Object... args) {
    super(cause, message, args);
  }

  /**
   * Constructs a new exception with the specified detail message and challenge.
   *
   * @param message the detail message.
   * @param challenge the challenge.
   */
  public UnauthorizedException(String message, String challenge) {
    super(message);
    challenges.add(challenge);
  }

  /**
   * Get the challenge of the exception.
   *
   * @return the challenge.
   */
  public List<String> getChallenges() {
    return challenges;
  }
}
