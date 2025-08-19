/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.config;

/** Constants used for configuration. */
public final class ConfigConstants {

  private ConfigConstants() {}

  /** HTTP Server port, reused by Gravitino server and Iceberg REST server */
  public static final String WEBSERVER_HTTP_PORT = "httpPort";
  /** HTTPS Server port, reused by Gravitino server and Iceberg REST server */
  public static final String WEBSERVER_HTTPS_PORT = "httpsPort";

  /** The value of messages used to indicate that the configuration is set to an empty value. */
  public static final String NOT_BLANK_ERROR_MSG = "The value can't be blank";

  /** The value of messages used to indicate that the configuration is not set. */
  public static final String NOT_NULL_ERROR_MSG = "The value can't be null";

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

  /** The version number for the 0.5.1 release. */
  public static final String VERSION_0_5_1 = "0.5.1";

  /** The version number for the 0.5.2 release. */
  public static final String VERSION_0_5_2 = "0.5.2";

  /** The version number for the 0.6.0 release. */
  public static final String VERSION_0_6_0 = "0.6.0";

  /** The version number for the 0.7.0 release. */
  public static final String VERSION_0_7_0 = "0.7.0";

  /** The version number for the 0.8.0 release. */
  public static final String VERSION_0_8_0 = "0.8.0";

  /** The version number for the 0.9.0 release. */
  public static final String VERSION_0_9_0 = "0.9.0";

  /** The version number for the 1.0.0 release. */
  public static final String VERSION_1_0_0 = "1.0.0";

  /** The current version of backend storage initialization script. */
  public static final String CURRENT_SCRIPT_VERSION = VERSION_1_0_0;
}
