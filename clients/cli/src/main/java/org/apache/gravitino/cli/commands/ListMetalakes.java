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

package org.apache.gravitino.cli.commands;

import org.apache.gravitino.Metalake;
import org.apache.gravitino.client.GravitinoAdminClient;

/** Lists all metalakes. */
public class ListMetalakes extends Command {

  /**
   * List all metalakes.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param outputFormat The output format.
   */
  public ListMetalakes(String url, boolean ignoreVersions, String outputFormat) {
    super(url, ignoreVersions, outputFormat);
  }

  /** Lists all metalakes. */
  @Override
  public void handle() {
    Metalake[] metalakes;
    try {
      GravitinoAdminClient client = buildAdminClient();
      metalakes = client.listMetalakes();
      output(metalakes);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }
  }
}
