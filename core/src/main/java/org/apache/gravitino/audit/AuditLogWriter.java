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

import java.io.Closeable;
import java.util.Map;
import org.apache.gravitino.listener.api.event.Event;

/**
 * Interface for writing the audit log, which can write to different storage, such as file,
 * database,mq.
 */
public interface AuditLogWriter extends Closeable {

  /** @return formatter. */
  Formatter getFormatter();

  /**
   * Initialize the writer with the given configuration.
   *
   * @param formatter formatter of type {@link Formatter} used to format the output
   * @param properties map of configuration properties
   */
  void init(Formatter formatter, Map<String, String> properties);

  /**
   * Write the audit event to storage.
   *
   * @param auditLog the {@link AuditLog} instance representing the event to be written
   */
  void doWrite(AuditLog auditLog);

  /**
   * Write the audit event to storage.
   *
   * @param event the audit {@link Event} to be written
   */
  default void write(Event event) {
    doWrite(getFormatter().format(event));
  }

  /**
   * Define the name of the writer, which related to audit writer configuration. Audit log writer
   * configuration start with: gravitino.audit.log.writer.${name}.*
   *
   * @return the name of the writer
   */
  String name();
}
