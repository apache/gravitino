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
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.messaging.TopicChange;

/** Update the comment of a topic. */
public class UpdateTopicComment extends Command {

  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String topic;
  protected final String comment;

  /**
   * Update the comment of a topic.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param topic The name of the topic.
   * @param comment New metalake comment.
   */
  public UpdateTopicComment(
      String url,
      boolean ignoreVersions,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String comment) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.topic = topic;
    this.comment = comment;
  }

  /** Update the comment of a topic. */
  @Override
  public void handle() {
    NameIdentifier name = NameIdentifier.of(schema, topic);

    try {
      GravitinoClient client = buildClient(metalake);
      TopicChange change = TopicChange.updateComment(comment);
      client.loadCatalog(catalog).asTopicCatalog().alterTopic(name, change);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException err) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (NoSuchTopicException err) {
      exitWithError(ErrorMessages.UNKNOWN_TOPIC);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    System.out.println(topic + " comment changed.");
  }
}
