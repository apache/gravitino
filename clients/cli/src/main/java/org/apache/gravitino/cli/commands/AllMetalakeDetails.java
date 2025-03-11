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
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.client.GravitinoAdminClient;

public class AllMetalakeDetails extends Command {

  /**
   * Parameters needed to list all metalakes.
   *
   * @param context The command context.
   */
  public AllMetalakeDetails(CommandContext context) {
    super(context);
  }

  /** Displays the name and comment of all metalakes. */
  @Override
  public void handle() {
    Metalake[] metalakes = new Metalake[0];
    try {
      GravitinoAdminClient client = buildAdminClient();
      metalakes = client.listMetalakes();
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    List<String> metalakeDetails = new ArrayList<>();
    for (int i = 0; i < metalakes.length; i++) {
      metalakeDetails.add(metalakes[i].name() + "," + metalakes[i].comment());
    }

    String all = Joiner.on(System.lineSeparator()).join(metalakeDetails);

    printResults(all);
  }
}
