/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.service;

/** The service class for schema metadata. It provides the basic database operations for schema. */
public class SchemaMetaService {
  private static final SchemaMetaService INSTANCE = new SchemaMetaService();

  public static SchemaMetaService getInstance() {
    return INSTANCE;
  }

  private SchemaMetaService() {}
}
