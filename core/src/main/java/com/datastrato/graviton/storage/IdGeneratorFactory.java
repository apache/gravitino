/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

import static com.datastrato.graviton.Configs.DEFAULT_ID_GENERATOR;
import static com.datastrato.graviton.Configs.ID_GERNERATOR;

import com.datastrato.graviton.Config;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for creating instances of IdGenerator implementations. We would
 * implement different IdGenerator implementations to generate different types of ids in the future.
 */
public class IdGeneratorFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(IdGeneratorFactory.class);

  // Maybe we can use guava cache and lazy loading to avoid loading all the id generators at once.
  // Currently, As a few id generators are available, we can just load them all at once.
  private static final Map<String, IdGenerator> NAME_TO_ID_GENERATOR =
      ImmutableMap.of("random", new RandomIdGenerator());

  public static IdGenerator getIdGeneratorByName(Config config) {
    String name = config.get(ID_GERNERATOR);
    if (name == null) {
      LOGGER.warn("No id generator specified, using random id generator");
      name = DEFAULT_ID_GENERATOR;
    }

    return NAME_TO_ID_GENERATOR.get(name);
  }
}
