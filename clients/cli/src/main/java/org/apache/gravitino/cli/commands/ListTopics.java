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

import java.util.Arrays;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.messaging.Topic;

/** List the topics. */
public class ListTopics extends Command {

  /** The name of the metalake. */
  protected final String metalake;
  /** The name of the catalog. */
  protected final String catalog;
  /** The name of the schema. */
  protected final String schema;

  /**
   * List the names of all topics in a schema.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   */
  public ListTopics(CommandContext context, String metalake, String catalog, String schema) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
  }

  /** List the names of all topics in a schema. */
  @Override
  public void handle() {
    NameIdentifier[] topics = new NameIdentifier[0];
    Namespace name = Namespace.of(schema);

    try {
      GravitinoClient client = buildClient(metalake);
      topics = client.loadCatalog(catalog).asTopicCatalog().listTopics(name);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    if (topics.length == 0) {
      printInformation("No topics exist.");
    } else {
      Topic[] topicObjects =
          Arrays.stream(topics).map(ident -> getTopic(ident.name())).toArray(Topic[]::new);
      printResults(topicObjects);
    }
  }

  private Topic getTopic(String name) {
    return new Topic() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public Audit auditInfo() {
        return null;
      }
    };
  }
}
