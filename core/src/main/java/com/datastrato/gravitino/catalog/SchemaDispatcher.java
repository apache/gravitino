/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.schema.SupportsSchemas;

/**
 * {@code SchemaDispatcher} interface acts as a specialization of the {@link SupportsSchemas}
 * interface. This interface is designed to potentially add custom behaviors or operations related
 * to dispatching or handling schema-related events or actions that are not covered by the standard
 * {@code SupportsSchemas} operations.
 */
public interface SchemaDispatcher extends SupportsSchemas {}
