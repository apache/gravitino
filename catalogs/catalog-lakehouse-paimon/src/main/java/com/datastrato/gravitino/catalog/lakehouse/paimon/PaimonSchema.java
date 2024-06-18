/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.connector.BaseSchema;
import lombok.ToString;

/**
 * Implementation of {@link Schema} that represents a Paimon Schema (Database) entity in the Paimon
 * schema.
 */
@ToString
public class PaimonSchema extends BaseSchema {

  private PaimonSchema() {}
}
