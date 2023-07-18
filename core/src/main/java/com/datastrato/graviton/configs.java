/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton;

import com.datastrato.graviton.config.ConfigBuilder;
import com.datastrato.graviton.config.ConfigEntry;

public interface configs {

  ConfigEntry<String> ENTITY_STORE =
      new ConfigBuilder("graviton.entity.store")
          .doc("The entity store to use")
          .version("0.1.0")
          .stringConf()
          // TODO. Change this when we have a EntityStore implementation. @Jerry
          .createWithDefault("in-memory");

  ConfigEntry<String> ENTITY_SERDE =
      new ConfigBuilder("graviton.entity.serde")
          .doc("The entity serde to use")
          .version("0.1.0")
          .stringConf()
          .createWithDefault("proto");
}
