/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.config;

/** Constants used for configuration. */
public interface ConfigConstants {

  /** The value of messages used to indicate that the configuration is not set. */
  String NOT_BLANK_ERROR_MSG = "The value can't be blank";

  /** The value of messages used to indicate that the configuration should be a positive number. */
  String POSITIVE_NUMBER_ERROR_MSG = "The value must be a positive number";

  /**
   * The value of messages used to indicate that the configuration should be a non-negative number.
   */
  String NON_NEGATIVE_NUMBER_ERROR_MSG = "The value must be a non-negative number";
}
