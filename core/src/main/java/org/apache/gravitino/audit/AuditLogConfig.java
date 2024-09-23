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

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.utils.MapUtils;

public class AuditLogConfig extends Config {
  public static final String AUDIT_LOG_ENABLED = "enable";
  static final String AUDIT_LOG_WRITER_PREFIX = "writer.";
  static final String AUDIT_FORMATTER_PREFIX = "formatter.";
  static final String AUDIT_LOG_WRITER_CLASS_NAME = AUDIT_LOG_WRITER_PREFIX + "class";
  static final String AUDIT_LOG_FORMATTER_CLASS_NAME = AUDIT_FORMATTER_PREFIX + "class";

  static final ConfigEntry<Boolean> AUDIT_LOG_ENABLED_CONF =
      new ConfigBuilder(AUDIT_LOG_ENABLED)
          .doc("Gravitino audit log enable flag")
          .version(ConfigConstants.VERSION_0_7_0)
          .booleanConf()
          .createWithDefault(false);

  static final ConfigEntry<String> WRITER_CLASS_NAME =
      new ConfigBuilder(AUDIT_LOG_WRITER_CLASS_NAME)
          .doc("Gravitino audit log writer class name")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault("");

  static final ConfigEntry<String> FORMATTER_CLASS_NAME =
      new ConfigBuilder(AUDIT_LOG_FORMATTER_CLASS_NAME)
          .doc("Gravitino event log formatter class name")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault("");

  public Boolean isAuditEnabled() {
    return get(AUDIT_LOG_ENABLED_CONF);
  }

  public String getWriterClassName() {
    return get(WRITER_CLASS_NAME);
  }

  public String getAuditLogFormatterClassName() {
    return get(FORMATTER_CLASS_NAME);
  }

  public Map<String, String> getWriterProperties(Map<String, String> properties) {
    return MapUtils.getPrefixMap(properties, AUDIT_LOG_WRITER_PREFIX);
  }

  public AuditLogConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }
}
