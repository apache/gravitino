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

import java.util.Map;
import org.apache.gravitino.cli.CommandContext;

/** List the properties of a metalake. */
public class ListProperties extends Command {

  /**
   * List the properties of an entity.
   *
   * @param context The command context.
   */
  public ListProperties(CommandContext context) {
    super(context);
  }

  @Override
  public void handle() {
    /* Do nothing */
  }

  /**
   * List the properties of an entity.
   *
   * @param properties The name, value pairs of properties.
   */
  public void printProperties(Map<String, String> properties) {
    StringBuilder all = new StringBuilder();

    for (Map.Entry<String, String> property : properties.entrySet()) {
      all.append(property.getKey() + "," + property.getValue() + System.lineSeparator());
    }

    System.out.print(all);
  }
}
