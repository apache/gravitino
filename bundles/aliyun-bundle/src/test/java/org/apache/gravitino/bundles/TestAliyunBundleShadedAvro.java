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
package org.apache.gravitino.bundles;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

class TestAliyunBundleShadedAvro {
  private static final File BUNDLE = new File(System.getProperty("shadowJarPath"));

  @Test
  void testBundleExcludesShadedAvro() throws Exception {
    try (JarFile jar = new JarFile(BUNDLE)) {
      assertFalse(
          jar.stream()
              .anyMatch(
                  entry ->
                      entry.getName().startsWith("org/apache/hadoop/shaded/org/apache/avro/")
                          || entry.getName().startsWith("META-INF/maven/org.apache.avro/")),
          BUNDLE.getName());
    }
  }

  @Test
  void testHadoopFileSystemWorksWithoutShadedAvro(@TempDir Path dir) throws Exception {
    URL[] urls = {
      BUNDLE.toURI().toURL(),
      LoggerFactory.class.getProtectionDomain().getCodeSource().getLocation()
    };
    byte[] data = "gravitino".getBytes(StandardCharsets.UTF_8);
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    // The ordinary test classpath also contains unshaded Hadoop, so load only the bundle.
    try (URLClassLoader loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
      thread.setContextClassLoader(loader);
      Class<?> confClass = loader.loadClass("org.apache.hadoop.conf.Configuration");
      Class<?> pathClass = loader.loadClass("org.apache.hadoop.fs.Path");
      Class<?> fsClass = loader.loadClass("org.apache.hadoop.fs.FileSystem");
      // Path and Text are annotated with the removed Avro annotations.
      pathClass.getAnnotations();
      loader.loadClass("org.apache.hadoop.io.Text").getAnnotations();

      // Creating the bundled file system does not contact a remote service.
      Object cloudFs =
          loader
              .loadClass("org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")
              .getConstructor()
              .newInstance();
      assertTrue(fsClass.isInstance(cloudFs));

      Object conf = confClass.getConstructor().newInstance();
      Object fs = fsClass.getMethod("getLocal", confClass).invoke(null, conf);
      Object file =
          pathClass
              .getConstructor(String.class)
              .newInstance(dir.resolve("data").toUri().toString());
      Method create = fsClass.getMethod("create", pathClass);
      try (OutputStream out = (OutputStream) create.invoke(fs, file)) {
        out.write(data);
      }
      Method open = fsClass.getMethod("open", pathClass);
      try (InputStream in = (InputStream) open.invoke(fs, file)) {
        assertArrayEquals(data, in.readAllBytes());
      }
      fsClass.getMethod("close").invoke(fs);
    } finally {
      thread.setContextClassLoader(previous);
    }
  }
}
