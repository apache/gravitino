package org.apache.gravitino.audit;

import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.utils.MapUtils;

public class AuditLogConfig extends Config {
  public static final String AUDIT_LOG_PREFIX = "gravitino.audit.";

  public static final String AUDIT_LOG_ENABLED = "enable";
  static final String AUDIT_LOG_WRITER_PREFIX = "writer.";
  static final String AUDIT_FORMATTER_PREFIX = "formatter.";
  static final String AUDIT_LOG_WRITER_CLASS_NAME = AUDIT_LOG_WRITER_PREFIX + "class";
  static final String AUDIT_LOG_FORMATTER_CLASS_NAME = AUDIT_FORMATTER_PREFIX + "class";

  static final ConfigEntry<String> AUDIT_LOG_ENABLED_CONF =
      new ConfigBuilder(AUDIT_LOG_ENABLED)
          .doc("Gravitino event audit log writer class")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .createWithDefault("false");

  static final ConfigEntry<String> WRITER_CLASS_NAME =
      new ConfigBuilder(AUDIT_LOG_WRITER_CLASS_NAME)
          .doc("Gravitino event audit log writer class")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .createWithDefault("");

  static final ConfigEntry<String> FORMATTER_CLASS_NAME =
      new ConfigBuilder(AUDIT_LOG_FORMATTER_CLASS_NAME)
          .doc("Gravitino event audit log writer class")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .createWithDefault("");

  public Boolean isAuditEnabled() {
    return get(AUDIT_LOG_ENABLED_CONF).equals("true");
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
