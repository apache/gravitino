/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog;

/**
 * {@code CatalogDispatcher} interface acts as a specialization of the {@link SupportsCatalogs}
 * interface. This interface is designed to potentially add custom behaviors or operations related
 * to dispatching or handling catalog-related events or actions that are not covered by the standard
 * {@code SupportsCatalogs} operations.
 */
public interface CatalogDispatcher extends SupportsCatalogs {}
