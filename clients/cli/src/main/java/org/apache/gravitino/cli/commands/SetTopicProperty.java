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
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.messaging.TopicChange;

/** Set a property of a topic. */
public class SetTopicProperty extends Command {

  protected final String metalake;
  protected final String catalog;
  protected final String schema;
  protected final String topic;
  protected final String property;
  protected final String value;

  /**
   * Set a property of a topic.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param topic The name of the topic.
   * @param property The name of the property.
   * @param value The value of the property.
   */
  public SetTopicProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String property,
      String value) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.topic = topic;
    this.property = property;
    this.value = value;
  }

  /** Set a property of a topic. */
  @Override
  public void handle() {
    NameIdentifier name = NameIdentifier.of(schema, topic);

    try {
      GravitinoClient client = buildClient(metalake);
      TopicChange change = TopicChange.setProperty(property, value);
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

    printInformation(property + " property set.");
  }

  @Override
  public Command validate() {
    validatePropertyAndValue(property, value);
    return super.validate();
  }
}
