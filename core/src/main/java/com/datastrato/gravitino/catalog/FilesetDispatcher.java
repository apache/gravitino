/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.file.FilesetCatalog;

/**
 * {@code FilesetDispatcher} interface acts as a specialization of the {@link FilesetCatalog}
 * interface. This interface is designed to potentially add custom behaviors or operations related
 * to dispatching or handling fileset-related events or actions that are not covered by the standard
 * {@code FilesetCatalog} operations.
 */
public interface FilesetDispatcher extends FilesetCatalog {}
