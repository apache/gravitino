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

package org.apache.gravitino.audit;

import org.apache.gravitino.listener.api.event.AlterCatalogEvent;
import org.apache.gravitino.listener.api.event.AlterCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.AlterFilesetEvent;
import org.apache.gravitino.listener.api.event.AlterFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.AlterMetalakeEvent;
import org.apache.gravitino.listener.api.event.AlterMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.AlterSchemaEvent;
import org.apache.gravitino.listener.api.event.AlterSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.AlterTableEvent;
import org.apache.gravitino.listener.api.event.AlterTableFailureEvent;
import org.apache.gravitino.listener.api.event.AlterTopicEvent;
import org.apache.gravitino.listener.api.event.AlterTopicFailureEvent;
import org.apache.gravitino.listener.api.event.CreateCatalogEvent;
import org.apache.gravitino.listener.api.event.CreateCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.CreateFilesetEvent;
import org.apache.gravitino.listener.api.event.CreateFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.CreateMetalakeEvent;
import org.apache.gravitino.listener.api.event.CreateMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.CreateSchemaEvent;
import org.apache.gravitino.listener.api.event.CreateSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.CreateTableEvent;
import org.apache.gravitino.listener.api.event.CreateTableFailureEvent;
import org.apache.gravitino.listener.api.event.CreateTopicEvent;
import org.apache.gravitino.listener.api.event.CreateTopicFailureEvent;
import org.apache.gravitino.listener.api.event.DropCatalogEvent;
import org.apache.gravitino.listener.api.event.DropCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.DropFilesetEvent;
import org.apache.gravitino.listener.api.event.DropFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.DropMetalakeEvent;
import org.apache.gravitino.listener.api.event.DropMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.DropSchemaEvent;
import org.apache.gravitino.listener.api.event.DropSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.DropTableEvent;
import org.apache.gravitino.listener.api.event.DropTableFailureEvent;
import org.apache.gravitino.listener.api.event.DropTopicEvent;
import org.apache.gravitino.listener.api.event.DropTopicFailureEvent;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.GetFileLocationEvent;
import org.apache.gravitino.listener.api.event.GetFileLocationFailureEvent;
import org.apache.gravitino.listener.api.event.GetPartitionEvent;
import org.apache.gravitino.listener.api.event.GetPartitionFailureEvent;
import org.apache.gravitino.listener.api.event.ListCatalogEvent;
import org.apache.gravitino.listener.api.event.ListCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.ListFilesetEvent;
import org.apache.gravitino.listener.api.event.ListFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.ListMetalakeEvent;
import org.apache.gravitino.listener.api.event.ListMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.ListPartitionEvent;
import org.apache.gravitino.listener.api.event.ListPartitionFailureEvent;
import org.apache.gravitino.listener.api.event.ListSchemaEvent;
import org.apache.gravitino.listener.api.event.ListSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.ListTableEvent;
import org.apache.gravitino.listener.api.event.ListTableFailureEvent;
import org.apache.gravitino.listener.api.event.ListTopicEvent;
import org.apache.gravitino.listener.api.event.ListTopicFailureEvent;
import org.apache.gravitino.listener.api.event.LoadCatalogEvent;
import org.apache.gravitino.listener.api.event.LoadCatalogFailureEvent;
import org.apache.gravitino.listener.api.event.LoadFilesetEvent;
import org.apache.gravitino.listener.api.event.LoadFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.LoadMetalakeEvent;
import org.apache.gravitino.listener.api.event.LoadMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.LoadSchemaEvent;
import org.apache.gravitino.listener.api.event.LoadSchemaFailureEvent;
import org.apache.gravitino.listener.api.event.LoadTableEvent;
import org.apache.gravitino.listener.api.event.LoadTableFailureEvent;
import org.apache.gravitino.listener.api.event.LoadTopicEvent;
import org.apache.gravitino.listener.api.event.LoadTopicFailureEvent;
import org.apache.gravitino.listener.api.event.PartitionExistsEvent;
import org.apache.gravitino.listener.api.event.PartitionExistsFailureEvent;
import org.apache.gravitino.listener.api.event.PurgePartitionEvent;
import org.apache.gravitino.listener.api.event.PurgePartitionFailureEvent;
import org.apache.gravitino.listener.api.event.PurgeTableEvent;
import org.apache.gravitino.listener.api.event.PurgeTableFailureEvent;

/** The interface define unified audit log schema. */
public interface AuditLog {
  /**
   * The user who do the operation.
   *
   * @return user name.
   */
  String user();

  /**
   * The operation name.
   *
   * @return operation name.
   */
  Operation operation();

  /**
   * The identifier of the resource.
   *
   * @return resource identifier name.
   */
  String identifier();

  /**
   * The timestamp of the operation.
   *
   * @return operation timestamp.
   */
  long timestamp();

  /**
   * The status of the operation.
   *
   * @return operation status.
   */
  Status status();

  /** Define user metadata operation. */
  enum Operation {
    CREATE_METALAKE,

    ALTER_METALAKE,

    DROP_METALAKE,

    LOAD_METALAKE,

    LIST_METALAKE,

    CREATE_CATALOG,

    LOAD_CATALOG,

    ALTER_CATALOG,

    DROP_CATALOG,

    LIST_CATALOG,

    CREATE_SCHEMA,

    ALTER_SCHEMA,

    DROP_SCHEMA,

    LOAD_SCHEMA,

    LIST_SCHEMA,

    CREATE_TABLE,

    ALTER_TABLE,

    DROP_TABLE,

    LOAD_TABLE,

    PURGE_TABLE,

    LIST_TABLE,

    PARTITION_EXIST,

    PURGE_PARTITION,

    LIST_PARTITION,

    GET_PARTITION,

    CREATE_TOPIC,

    ALTER_TOPIC,

    DROP_TOPIC,

    LOAD_TOPIC,

    LIST_TOPIC,

    GET_FILE_LOCATION,

    CREATE_FILESET,

    ALTER_FILESET,

    DROP_FILESET,

    LOAD_FILESET,

    LIST_FILESET,

    UNKNOWN_OPERATION;

    public static Operation fromEvent(Event event) {
      if (event instanceof CreateMetalakeEvent || event instanceof CreateMetalakeFailureEvent) {
        return CREATE_METALAKE;
      } else if (event instanceof AlterMetalakeEvent
          || event instanceof AlterMetalakeFailureEvent) {
        return ALTER_METALAKE;
      } else if (event instanceof DropMetalakeEvent || event instanceof DropMetalakeFailureEvent) {
        return DROP_METALAKE;
      } else if (event instanceof LoadMetalakeEvent || event instanceof LoadMetalakeFailureEvent) {
        return LOAD_METALAKE;
      } else if (event instanceof ListMetalakeEvent || event instanceof ListMetalakeFailureEvent) {
        return LIST_METALAKE;
      } else if (event instanceof CreateCatalogEvent
          || event instanceof CreateCatalogFailureEvent) {
        return CREATE_CATALOG;
      } else if (event instanceof AlterCatalogEvent || event instanceof AlterCatalogFailureEvent) {
        return ALTER_CATALOG;
      } else if (event instanceof DropCatalogEvent || event instanceof DropCatalogFailureEvent) {
        return DROP_CATALOG;
      } else if (event instanceof LoadCatalogEvent || event instanceof LoadCatalogFailureEvent) {
        return LOAD_CATALOG;
      } else if (event instanceof ListCatalogEvent || event instanceof ListCatalogFailureEvent) {
        return LIST_CATALOG;
      } else if (event instanceof CreateSchemaEvent || event instanceof CreateSchemaFailureEvent) {
        return CREATE_SCHEMA;
      } else if (event instanceof AlterSchemaEvent || event instanceof AlterSchemaFailureEvent) {
        return ALTER_SCHEMA;
      } else if (event instanceof DropSchemaEvent || event instanceof DropSchemaFailureEvent) {
        return DROP_SCHEMA;
      } else if (event instanceof LoadSchemaEvent || event instanceof LoadSchemaFailureEvent) {
        return LOAD_SCHEMA;
      } else if (event instanceof ListSchemaEvent || event instanceof ListSchemaFailureEvent) {
        return LIST_SCHEMA;
      } else if (event instanceof CreateTableEvent || event instanceof CreateTableFailureEvent) {
        return CREATE_TABLE;
      } else if (event instanceof AlterTableEvent || event instanceof AlterTableFailureEvent) {
        return ALTER_TABLE;
      } else if (event instanceof DropTableEvent || event instanceof DropTableFailureEvent) {
        return DROP_TABLE;
      } else if (event instanceof LoadTableEvent || event instanceof LoadTableFailureEvent) {
        return LOAD_TABLE;
      } else if (event instanceof PurgeTableEvent || event instanceof PurgeTableFailureEvent) {
        return PURGE_TABLE;
      } else if (event instanceof ListTableEvent || event instanceof ListTableFailureEvent) {
        return LIST_TABLE;
      } else if (event instanceof PartitionExistsEvent
          || event instanceof PartitionExistsFailureEvent) {
        return PARTITION_EXIST;
      } else if (event instanceof PurgePartitionEvent
          || event instanceof PurgePartitionFailureEvent) {
        return PURGE_PARTITION;
      } else if (event instanceof ListPartitionEvent
          || event instanceof ListPartitionFailureEvent) {
        return LIST_PARTITION;
      } else if (event instanceof GetPartitionEvent || event instanceof GetPartitionFailureEvent) {
        return GET_PARTITION;
      } else if (event instanceof CreateTopicEvent || event instanceof CreateTopicFailureEvent) {
        return CREATE_TOPIC;
      } else if (event instanceof AlterTopicEvent || event instanceof AlterTopicFailureEvent) {
        return ALTER_TOPIC;
      } else if (event instanceof DropTopicEvent || event instanceof DropTopicFailureEvent) {
        return DROP_TOPIC;
      } else if (event instanceof LoadTopicEvent || event instanceof LoadTopicFailureEvent) {
        return LOAD_TOPIC;
      } else if (event instanceof ListTopicEvent || event instanceof ListTopicFailureEvent) {
        return LIST_TOPIC;
      } else if (event instanceof CreateFilesetEvent
          || event instanceof CreateFilesetFailureEvent) {
        return CREATE_FILESET;
      } else if (event instanceof AlterFilesetEvent || event instanceof AlterFilesetFailureEvent) {
        return ALTER_FILESET;
      } else if (event instanceof DropFilesetEvent || event instanceof DropFilesetFailureEvent) {
        return DROP_FILESET;
      } else if (event instanceof GetFileLocationEvent
          || event instanceof GetFileLocationFailureEvent) {
        return GET_FILE_LOCATION;
      } else if (event instanceof LoadFilesetEvent || event instanceof LoadFilesetFailureEvent) {
        return LOAD_FILESET;
      } else if (event instanceof ListFilesetEvent || event instanceof ListFilesetFailureEvent) {
        return LIST_FILESET;
      } else {
        return UNKNOWN_OPERATION;
      }
    }
  }

  enum Status {
    SUCCESS,
    FAILURE
  }
}
