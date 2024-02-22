/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.config;

/** Constants used for configuration. */
public final class ConfigConstants {

  private ConfigConstants() {}

  /** The value of messages used to indicate that the configuration is not set. */
  public static final String NOT_BLANK_ERROR_MSG = "The value can't be blank";

  /** The value of messages used to indicate that the configuration should be a positive number. */
  public static final String POSITIVE_NUMBER_ERROR_MSG = "The value must be a positive number";

  /**
   * The value of messages used to indicate that the configuration should be a non-negative number.
   */
  public static final String NON_NEGATIVE_NUMBER_ERROR_MSG =
      "The value must be a non-negative number";

  /** The version number for the 0.1.0 release. */
  public static final String VERSION_0_1_0 = "0.1.0";
  /** The version number for the 0.2.0 release. */
  public static final String VERSION_0_2_0 = "0.2.0";
  /** The version number for the 0.3.0 release. */
  public static final String VERSION_0_3_0 = "0.3.0";
  /** The version number for the 0.4.0 release. */
  public static final String VERSION_0_4_0 = "0.4.0";
  /** The version number for the 0.5.0 release. */
  public static final String VERSION_0_5_0 = "0.5.0";
}
