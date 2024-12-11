package org.apache.gravitino.flink.connector.paimon;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class GravitinoPaimonCatalogFactoryOptions {

  /** Identifier for the {@link GravitinoPaimonCatalog}. */
  public static final String IDENTIFIER = "lakehouse-paimon";

  public static ConfigOption<String> backendType =
      ConfigOptions.key("catalog-backend")
          .stringType()
          .defaultValue("fileSystem")
          .withDescription("");
}
