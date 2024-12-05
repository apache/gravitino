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

import java.util.List;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchUserException;

public class GroupDetails extends Command {

  protected final String metalake;
  protected final String group;

  /**
   * Displays the roles in a group.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param group The name of the group.
   */
  public GroupDetails(String url, boolean ignoreVersions, String metalake, String group) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.group = group;
  }

  /** Displays the roles of a specified group. */
  @Override
  public void handle() {
    List<String> roles = null;

    try {
      GravitinoClient client = buildClient(metalake);
      roles = client.getGroup(group).roles();
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (NoSuchUserException err) {
      System.err.println(ErrorMessages.UNKNOWN_GROUP);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    String all = String.join(",", roles);

    System.out.println(all.toString());
  }
}
