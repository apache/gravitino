/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.connector.TableCatalog;

/**
 * {@code TableDispatcher} interface acts as a specialization of the {@link TableCatalog} interface.
 * This interface is designed to potentially add custom behaviors or operations related to
 * dispatching or handling table-related events or actions that are not covered by the standard
 * {@code TableCatalog} operations.
 */
public interface TableDispatcher extends TableCatalog {}
