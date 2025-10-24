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

package org.apache.gravitino.cli.options;

import org.apache.gravitino.cli.GravitinoOptions;
import picocli.CommandLine;

/** Defines the options for setting a property in key=value format. */
public class PropertyOptions {
  /** The property option, use -P/--property to set the property name */
  @CommandLine.Option(
      names = {"-P", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.PROPERTY},
      required = true,
      description = "The property to set in key=value format")
  public String property;

  /** The value option, use -V/--value to set the property value */
  @CommandLine.Option(
      names = {"-V", GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.VALUE},
      required = true,
      description = "The value to set in key=value format")
  public String value;
}
