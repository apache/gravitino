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

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.client.GravitinoAdminClient;

/** Lists all metalakes. */
public class ListMetalakes extends Command {

  /**
   * List all metalakes.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   */
  public ListMetalakes(String url, boolean ignoreVersions) {
    super(url, ignoreVersions);
  }

  /** Lists all metalakes. */
  public void handle() {
    Metalake[] metalakes = new Metalake[0];
    try {
      GravitinoAdminClient client = buildAdminClient();
      metalakes = client.listMetalakes();
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    List<String> metalakeNames = new ArrayList<>();
    for (int i = 0; i < metalakes.length; i++) {
      metalakeNames.add(metalakes[i].name());
    }

    String all = Joiner.on(System.lineSeparator()).join(metalakeNames);

    System.out.println(all.toString());
  }
}
