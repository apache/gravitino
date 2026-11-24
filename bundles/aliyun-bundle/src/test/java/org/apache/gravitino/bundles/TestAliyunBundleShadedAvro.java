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
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

class TestAliyunBundleShadedAvro {
  private static final File BUNDLE = new File(System.getProperty("shadowJarPath"));

  @TempDir private Path dir;
  private ClassLoader previousLoader;
  private URLClassLoader loader;
  private Class<?> confClass;
  private Class<?> pathClass;
  private Class<?> fsClass;
  private Object conf;
  private Object fs;

  @BeforeEach
  void setUp() throws Exception {
    URL[] urls = {
      BUNDLE.toURI().toURL(),
      LoggerFactory.class.getProtectionDomain().getCodeSource().getLocation()
    };
    // The ordinary test classpath also contains unshaded Hadoop, so load only the bundle.
    loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader());
    previousLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(loader);
    confClass = loader.loadClass("org.apache.hadoop.conf.Configuration");
    pathClass = loader.loadClass("org.apache.hadoop.fs.Path");
    fsClass = loader.loadClass("org.apache.hadoop.fs.FileSystem");
    conf = confClass.getConstructor().newInstance();
    fs = fsClass.getMethod("getLocal", confClass).invoke(null, conf);
  }

  @AfterEach
  void tearDown() throws Exception {
    try {
      if (fs != null) {
        fsClass.getMethod("close").invoke(fs);
      }
      loader.close();
    } finally {
      Thread.currentThread().setContextClassLoader(previousLoader);
    }
  }

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
    // The Avro serializations are removed from the defaults together with the shaded Avro.
    assertEquals(
        "org.apache.hadoop.io.serializer.WritableSerialization",
        confClass.getMethod("get", String.class).invoke(conf, "io.serializations"));
  }

  @Test
  void testFileSystemWorksWithoutShadedAvro() throws Exception {
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

    byte[] data = "gravitino".getBytes(StandardCharsets.UTF_8);
    Object file = newPath("data");
    Method create = fsClass.getMethod("create", pathClass);
    try (OutputStream out = (OutputStream) create.invoke(fs, file)) {
      out.write(data);
    }
    Method open = fsClass.getMethod("open", pathClass);
    try (InputStream in = (InputStream) open.invoke(fs, file)) {
      assertArrayEquals(data, in.readAllBytes());
    }
  }

  @Test
  void testWritableSequenceFileWorksWithoutShadedAvro() throws Exception {
    Class<?> textClass = loader.loadClass("org.apache.hadoop.io.Text");
    Class<?> writableClass = loader.loadClass("org.apache.hadoop.io.Writable");
    Class<?> writerClass = loader.loadClass("org.apache.hadoop.io.SequenceFile$Writer");
    Class<?> readerClass = loader.loadClass("org.apache.hadoop.io.SequenceFile$Reader");
    Object file = newPath("data.seq");

    Object writer =
        loader
            .loadClass("org.apache.hadoop.io.SequenceFile")
            .getMethod("createWriter", fsClass, confClass, pathClass, Class.class, Class.class)
            .invoke(null, fs, conf, file, textClass, textClass);
    try {
      writerClass
          .getMethod("append", writableClass, writableClass)
          .invoke(writer, newText(textClass, "key"), newText(textClass, "value"));
    } finally {
      writerClass.getMethod("close").invoke(writer);
    }

    Object reader =
        readerClass.getConstructor(fsClass, pathClass, confClass).newInstance(fs, file, conf);
    try {
      Object key = newText(textClass, "");
      Object value = newText(textClass, "");
      Method next = readerClass.getMethod("next", writableClass, writableClass);
      assertTrue((Boolean) next.invoke(reader, key, value));
      assertEquals("key", key.toString());
      assertEquals("value", value.toString());
      assertFalse((Boolean) next.invoke(reader, key, value));
    } finally {
      readerClass.getMethod("close").invoke(reader);
    }
  }

  private Object newPath(String name) throws Exception {
    return pathClass.getConstructor(String.class).newInstance(dir.resolve(name).toUri().toString());
  }

  private static Object newText(Class<?> textClass, String value) throws Exception {
    return textClass.getConstructor(String.class).newInstance(value);
  }
}
