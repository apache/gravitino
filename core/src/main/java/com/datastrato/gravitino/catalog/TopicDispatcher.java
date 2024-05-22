/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.connector.TopicCatalog;

/**
 * {@code TopicDispatcher} interface acts as a specialization of the {@link TopicCatalog} interface.
 * This interface is designed to potentially add custom behaviors or operations related to
 * dispatching or handling topic-related events or actions that are not covered by the standard
 * {@code TopicCatalog} operations.
 */
public interface TopicDispatcher extends TopicCatalog {}
