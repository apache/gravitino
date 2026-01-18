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
package org.apache.gravitino.catalog.fileset;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.HDFSFileSystemProxy;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A FileSystem proxy that supports user impersonation for HDFS FileSystem */
public class ImpersonationHDFSFileSystemProxy extends HDFSFileSystemProxy {

  private static final Logger LOG = LoggerFactory.getLogger(ImpersonationHDFSFileSystemProxy.class);

  private final ProxyUserHandler proxyUserHandler;

  /**
   * Create a HDFSAuthenticationFileSystem with the given path and configuration. Supports both
   * simple and Kerberos authentication and has user impersonation capability.
   *
   * @param path the HDFS path
   * @param config the configuration map of Gravitino
   * @param handler the handle of retrieving proxy user
   */
  public ImpersonationHDFSFileSystemProxy(
      Path path, Map<String, String> config, ProxyUserHandler handler) {
    super();

    Preconditions.checkArgument(handler != null, "ProxyUserHandler cannot be null");
    this.proxyUserHandler = handler;
    initFileSystem(path, config);
  }

  protected UserGroupInformation getRequestUser() {
    if (!impersonationEnabled) {
      return super.getRequestUser();
    }

    UserGroupInformation requestUgi;
    String proxyUserName = proxyUserHandler.getProxyUser();
    if (!proxyUserName.contains("@")) {
      proxyUserName = String.format("%s@%s", proxyUserName, kerberosRealm);
    }
    requestUgi = UserGroupInformation.createProxyUser(proxyUserName, initUgi);
    LOG.debug("Using login user with impersonation: {}", initUgi.getUserName());
    return requestUgi;
  }

  interface ProxyUserHandler {
    String getProxyUser();
  }
}
