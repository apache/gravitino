/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.metalake;

/**
 * {@code MetalakeDispatcher} interface acts as a specialization of the {@link SupportsMetalakes}
 * interface. This interface is designed to potentially add custom behaviors or operations related
 * to dispatching or handling metalake-related events or actions that are not covered by the
 * standard {@code SupportsMetalakes} operations.
 */
public interface MetalakeDispatcher extends SupportsMetalakes {}
