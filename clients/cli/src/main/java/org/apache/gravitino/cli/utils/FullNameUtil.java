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

package org.apache.gravitino.cli.utils;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.FullName;

/** Utility class for helping with creating NameIdentifiers from {@link FullName}. */
public class FullNameUtil {

  /**
   * Returns a NameIdentifier for a model.
   *
   * @param fullName the {@link FullName} of the model.
   * @return a NameIdentifier for the model.
   */
  public static NameIdentifier toModel(FullName fullName) {
    String schema = fullName.getSchemaName();
    String model = fullName.getModelName();

    return NameIdentifier.of(schema, model);
  }

  /**
   * Returns a NameIdentifier for a table.
   *
   * @param fullName the {@link FullName} of the table.
   * @return a NameIdentifier for the table.
   */
  public static NameIdentifier toTable(FullName fullName) {
    String schema = fullName.getSchemaName();
    String table = fullName.getTableName();

    return NameIdentifier.of(schema, table);
  }

  /**
   * Returns a NameIdentifier for a fileset.
   *
   * @param fullName the {@link FullName} of the fileset.
   * @return a NameIdentifier for the fileset.
   */
  public static NameIdentifier toFileset(FullName fullName) {
    String schema = fullName.getSchemaName();
    String fileset = fullName.getFilesetName();

    return NameIdentifier.of(schema, fileset);
  }

  /**
   * Returns a NameIdentifier for a topic.
   *
   * @param fullName the {@link FullName} of the topic.
   * @return a NameIdentifier for the topic.
   */
  public static NameIdentifier toTopic(FullName fullName) {
    String schema = fullName.getSchemaName();
    String topic = fullName.getTopicName();

    return NameIdentifier.of(schema, topic);
  }
}
