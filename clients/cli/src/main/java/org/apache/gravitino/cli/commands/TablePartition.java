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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.partitions.Partition;

/** Displays the details of a table's partition. */
public class TablePartition extends TableCommand {

  protected final String schema;
  protected final String table;

  /**
   * Displays the details of a table's partition.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schenma.
   * @param table The name of the table.
   */
  public TablePartition(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    super(context, metalake, catalog);
    this.schema = schema;
    this.table = table;
  }

  /** Displays the name and properties of partition. */
  @Override
  public void handle() {
    Transform transforms[] = {};
    try {
      NameIdentifier name = NameIdentifier.of(schema, table);
      transforms = tableCatalog().loadTable(name).partitioning();
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }
    for (Transform transform : transforms) {
      Partition[] partitions = transform.assignments();
      if (partitions.length == 0) {
        printInformation("None");
      } else {
        for (Partition partition : partitions) {
          printResults(partition.name() + "," + partition.properties());
        }
      }
    }
  }
}
