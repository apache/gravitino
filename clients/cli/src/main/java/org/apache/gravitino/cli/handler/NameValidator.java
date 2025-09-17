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

package org.apache.gravitino.cli.handler;

import java.util.List;
import org.apache.gravitino.cli.CliFullName;

/** Interface for validating the name of a CLI command. */
public interface NameValidator {

  /**
   * Validates the name of a CLI command. if the name is valid, returns an empty list. Otherwise,
   * returns a list of missing entities.
   *
   * @param fullName the full name of the --name option
   * @return a list of missing entities if the name is invalid, otherwise an empty list.
   */
  List<String> getMissingEntities(CliFullName fullName);
}
