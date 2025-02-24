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

import org.apache.gravitino.Audit;
import org.apache.gravitino.cli.CommandContext;

public abstract class AuditCommand extends Command {

  /** @param context The command context. */
  public AuditCommand(CommandContext context) {
    super(context);
  }

  /* Overridden in parent - do nothing  */
  @Override
  public void handle() {}

  /**
   * Displays audit information for the given audit object.
   *
   * @param audit from a class that implements the Auditable interface.
   */
  public void displayAuditInfo(Audit audit) {
    String auditInfo =
        "creator,create_time,modified,modified_time"
            + System.lineSeparator()
            + audit.creator()
            + ","
            + audit.createTime()
            + ","
            + audit.lastModifier()
            + ","
            + audit.lastModifiedTime();

    printResults(auditInfo);
  }
}
