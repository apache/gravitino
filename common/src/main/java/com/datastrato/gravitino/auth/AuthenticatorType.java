/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.auth;

/** The type of authenticator for http/https request. */
public enum AuthenticatorType {
  /** No authentication. */
  NONE,

  /** Simple authentication. */
  SIMPLE,

  /** Authentication that uses OAuth. */
  OAUTH,

  /** Authentication that uses Kerberos. */
  KERBEROS
}
