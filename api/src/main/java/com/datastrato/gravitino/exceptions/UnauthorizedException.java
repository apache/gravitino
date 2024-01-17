/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.exceptions;

import com.google.common.collect.Lists;

import java.util.List;

/** Exception thrown when a user is not authorized to perform an action. */
public class UnauthorizedException extends GravitinoRuntimeException {

  List<String> challenges = Lists.newArrayList();

  public UnauthorizedException(String message) {
    super(message);
  }

  public UnauthorizedException(String message, Throwable cause) {
    super(message, cause);
  }

  public List<String> getChallenges() {
    return challenges;
  }

}
