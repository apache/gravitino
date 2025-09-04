/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.cli;

/** Represents the Kerberos data required to authenticate with a remote server. */
public class KerberosData {
  /** The Kerberos principal (e.g. a user or service identity). */
  protected final String principal;
  /** The path to the keytab file. */
  protected final String keytabFile;

  /**
   * Constructs an {@code KerberosData} instance with the specified principal and keytab file.
   *
   * @param principal the Kerberos principal (e.g. a user or service identity)
   * @param keytabFile the path to the keytab file
   */
  public KerberosData(String principal, String keytabFile) {
    this.principal = principal;
    this.keytabFile = keytabFile;
  }

  /**
   * Returns the Kerberos principal associated with this {@code KerberosData}.
   *
   * @return the principal
   */
  public String getPrincipal() {
    return principal;
  }

  /**
   * Returns the keytab file path associated with this {@code KerberosData}.
   *
   * @return the keytab file path
   */
  public String getKeytabFile() {
    return keytabFile;
  }
}
