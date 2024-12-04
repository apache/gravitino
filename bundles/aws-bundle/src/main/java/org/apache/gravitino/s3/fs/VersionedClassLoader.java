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

package org.apache.gravitino.s3.fs;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.IOUtils;

public class VersionedClassLoader extends URLClassLoader {

  private String version;

  public VersionedClassLoader(URL[] urls, ClassLoader parent, String version) {
    super(urls, parent);
    this.version = version;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public static VersionedClassLoader loadVersion(String version) throws Exception {

    // Default version 3.3 if the version is not provided
    String hadoopVersion = "hadoop3_3";
    String[] versionNumber = version.split("\\.");
    if (versionNumber.length < 2 || versionNumber.length > 3) {
      // Users may custom the version, so we need to handle the case that the version is not x.x
      URL[] urls = loadFile(hadoopVersion);
      return new VersionedClassLoader(
          urls, VersionedClassLoader.class.getClassLoader(), hadoopVersion);
    }

    int mainVersion = Integer.parseInt(versionNumber[0]);
    int subVersion = Integer.parseInt(versionNumber[1]);
    int lastVersion = versionNumber.length == 3 ? Integer.parseInt(versionNumber[2]) : -1;

    // Version < 2.10 will use hadoop2_7, version >= 3.3.1 will use hadoop3_3 and others
    // Use hadoop2_10
    if (mainVersion == 2 && subVersion < 10) {
      hadoopVersion = "hadoop2_7";
    } else if (mainVersion == 3 && ((subVersion == 3 && lastVersion > 0) || subVersion > 3)) {
      hadoopVersion = "hadoop3_3";
    } else {
      hadoopVersion = "hadoop2_10";
    }

    URL[] urls = loadFile(hadoopVersion);
    return new VersionedClassLoader(
        urls, VersionedClassLoader.class.getClassLoader(), hadoopVersion);
  }

  public static URL[] loadFile(String hadoopVersion) {
    try {
      ClassLoader classLoader = VersionedClassLoader.class.getClassLoader();

      String className = VersionedClassLoader.class.getName().replace('.', '/') + ".class";
      URL classUrl = classLoader.getResource(className);

      if (classUrl == null) {
        throw new RuntimeException("Class resource not found!");
      }

      List<URL> list = Lists.newArrayList();
      if (classUrl.getProtocol().equals("jar")) {
        String jarFilePath = classUrl.getFile().substring(5, classUrl.getFile().indexOf("!"));

        JarFile jarFile = new JarFile(jarFilePath);

        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
          JarEntry entry = entries.nextElement();
          if (entry.getName().startsWith(hadoopVersion) && entry.getName().endsWith(".jar.zip")) {
            InputStream inputStream = jarFile.getInputStream(entry);
            File tempFile = File.createTempFile(entry.getName(), ".jar");
            IOUtils.copy(inputStream, Files.newOutputStream(tempFile.toPath()));
            list.add(tempFile.toURI().toURL());
          }
        }

        jarFile.close();
        return list.toArray(new URL[0]);
      } else {
        throw new RuntimeException("Unsupported protocol: " + classUrl.getProtocol());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
