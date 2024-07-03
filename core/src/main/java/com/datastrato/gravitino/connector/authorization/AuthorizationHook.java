/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector.authorization;

import java.io.Closeable;

/**
 * authorization operations hooks interfaces. Note: Because each interface function needs to perform
 * multiple steps in the underlying permission system, the implementation method of these function
 * interface must be idempotent.
 */
public interface AuthorizationHook
    extends AuthorizationUserHook, AuthorizationRoleHook, Closeable {}
