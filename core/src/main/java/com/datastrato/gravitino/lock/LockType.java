/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

/**
 * Type of the lock. READ lock is a shared lock, while WRITE lock is an exclusive lock.
 *
 * <p>It's possible to acquire multiple READ locks at the same time, but only one WRITE lock can be
 * acquired at a time. please see {@link java.util.concurrent.locks.ReadWriteLock} for more details.
 */
public enum LockType {
  READ,
  WRITE
}
