package com.datastrato.graviton;

import com.datastrato.graviton.config.ConfigBuilder;
import com.datastrato.graviton.config.ConfigEntry;

public interface configs {

  ConfigEntry<String> ENTITY_STORE_TYPE =
      new ConfigBuilder("graviton.entityStore.type")
          .doc("The type of the entity store to use")
          .version("0.1.0")
          .stringConf()
          // TODO. Change this when we have a EntityStore implementation. @Jerry
          .createWithDefault("in-memory");
}
