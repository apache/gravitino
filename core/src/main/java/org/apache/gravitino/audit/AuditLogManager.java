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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * AuditLogManager is responsible for initializing the audit log writer and formatter,
 * which are used to write metadata audit logs.
 * */
public class AuditLogManager {

  private static final Logger LOG = LoggerFactory.getLogger(AuditLogManager.class);

  @VisibleForTesting private AuditLogWriter auditLogWriter;

  public void init(Config config, EventListenerManager eventBusManager) {
    if (!config.get(Configs.AUDIT_LOG_ENABLED_CONF)) {
      LOG.info("Audit log is not enabled");
      return;
    }
    String formatterClassName = config.get(Configs.AUDIT_LOG_FORMATTER_CLASS_NAME);
    Formatter formatter = loadFormatter(formatterClassName);
    LOG.info("Audit log formatter class name:{}", formatterClassName);

    String writerClassName = config.get(Configs.AUDIT_LOG_WRITER_CLASS_NAME);
    auditLogWriter =
        loadAuditLogWriter(
            writerClassName,
            config.getConfigsWithPrefix(Configs.AUDIT_LOG_WRITER_CONFIG_PREFIX),
            formatter);
    LOG.info("Audit log writer class name:{}", writerClassName);

    eventBusManager.addEventListener(
        "audit-log",
        new EventListenerPlugin() {

          @Override
          public void init(Map<String, String> properties) throws RuntimeException {}

          @Override
          public void start() throws RuntimeException {}

          @Override
          public void stop() throws RuntimeException {
            try {
              auditLogWriter.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void onPostEvent(Event event) throws RuntimeException {
            try {
              auditLogWriter.write(event);
            } catch (Exception e) {
              LOG.warn("Failed to write audit log {}.", event, e);
            }
          }

          @Override
          public Mode mode() {
            return Mode.ASYNC_ISOLATED;
          }
        });
  }

  private AuditLogWriter loadAuditLogWriter(
      String className, Map<String, String> config, Formatter formatter) {
    try {
      AuditLogWriter auditLogWriter =
          (AuditLogWriter) Class.forName(className).getDeclaredConstructor().newInstance();
      Map<String, String> writerConfig = MapUtils.getPrefixMap(config, auditLogWriter.name() + ".");
      auditLogWriter.init(formatter, writerConfig);
      return auditLogWriter;
    } catch (Exception e) {
      throw new GravitinoRuntimeException(e, "Failed to load audit log writer %s", className);
    }
  }

  private Formatter loadFormatter(String className) {
    try {
      return (Formatter) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new GravitinoRuntimeException(e, "Failed to load formatter class name %s", className);
    }
  }

  AuditLogWriter getAuditLogWriter() {
    return auditLogWriter;
  }
}
