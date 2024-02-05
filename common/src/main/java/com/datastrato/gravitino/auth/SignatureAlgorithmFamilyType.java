/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.auth;

/** Represents the type of signature algorithm family. */
public enum SignatureAlgorithmFamilyType {
  /** HMAC family of algorithms. */
  HMAC,

  /** RSA family of algorithms. */
  RSA,

  /** ECDSA family of algorithms. */
  ECDSA
}
