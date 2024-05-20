/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.SupportsPartitions;
import java.io.Closeable;

/**
 * A table operation interface that is used to trigger the operations of a table. This interface
 * should be mixed with other Table interface like {@link SupportsPartitions} to provide partition
 * operation, etc.
 */
@Evolving
public interface TableOperations extends Closeable {}
