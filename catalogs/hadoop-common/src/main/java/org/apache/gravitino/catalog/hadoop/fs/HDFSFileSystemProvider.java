/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.hadoop.fs;

import static org.apache.gravitino.catalog.hadoop.fs.Constants.BUILTIN_HDFS_FS_PROVIDER;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class HDFSFileSystemProvider implements FileSystemProvider {

  @Override
  public FileSystem getFileSystem(@Nonnull Path path, @Nonnull Map<String, String> config)
      throws IOException {
    Configuration configuration = FileSystemUtils.createConfiguration(GRAVITINO_BYPASS, config);
    return createFileSystemWithUgi(configuration);
  }

  @Override
  public String scheme() {
    return "hdfs";
  }

  @Override
  public String name() {
    return BUILTIN_HDFS_FS_PROVIDER;
  }

  public static FileSystem createFileSystemWithUgi(Configuration configuration) {
    try {
      UserGroupInformation.setConfiguration(configuration);
      String authType = configuration.get("hadoop.security.authentication", "simple");
      if (authType.equals("kerberos")) {
        UserGroupInformation.setConfiguration(configuration);

        UserGroupInformation ugi =
            UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                configuration.get("hadoop.security.authentication.kerberos.principal"),
                configuration.get("hadoop.security.authentication.kerberos.keytab"));

        return ugi.doAs(
            (java.security.PrivilegedExceptionAction<FileSystem>)
                () -> FileSystem.newInstance(configuration));
      } else {
        UserGroupInformation.setConfiguration(configuration);
        return FileSystem.newInstance(configuration);
      }
    } catch (Exception e) {
      throw new GravitinoRuntimeException("Failed to create HDFS FileSystem with UGI: %s", e);
    }
  }
}
