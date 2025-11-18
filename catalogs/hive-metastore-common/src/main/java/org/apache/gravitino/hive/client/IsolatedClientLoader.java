/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.hive.client;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IsolatedClientLoader {
  private static final Logger LOG = LoggerFactory.getLogger(IsolatedClientLoader.class);

  /** Creates isolated Hive client loaders by downloading the requested version from maven. */
  public static synchronized IsolatedClientLoader forVersion(
      String hiveMetastoreVersion, Map<String, String> config) throws IOException {
    HiveClient.HiveVersion hiveVersion = HiveClient.HiveVersion.valueOf(hiveMetastoreVersion);
    String jarPath = "/dev/null";
    if (hiveVersion == HiveClient.HiveVersion.HIVE2) {
      jarPath = "/home/ubuntu/git/gravitino/catalogs/hive-metastore2-libs/build/libs";
    } else if (hiveVersion == HiveClient.HiveVersion.HIVE3) {
      jarPath = "/home/ubuntu/git/gravitino/catalogs/hive-metastore3-libs/build/libs";
    } else {
      throw new IllegalArgumentException("Unsupported Hive version: " + hiveMetastoreVersion);
    }

    Path jarDir = Paths.get(jarPath);
    List<URL> jars =
        Files.list(jarDir)
            .filter(p -> p.toString().endsWith(".jar"))
            .map(
                p -> {
                  try {
                    return p.toUri().toURL();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    return new IsolatedClientLoader(
        hiveVersion, jars, config, Thread.currentThread().getContextClassLoader());
  }

  private final HiveClient.HiveVersion version;
  private final List<URL> execJars;
  private final Map<String, String> config;
  private final ClassLoader baseClassLoader;
  private final URLClassLoader classLoader;

  public IsolatedClientLoader(
      HiveClient.HiveVersion version,
      List<URL> execJars,
      Map<String, String> config,
      ClassLoader baseClassLoader) {

    this.version = version;
    this.execJars = execJars != null ? execJars : Collections.emptyList();
    this.config = config != null ? config : Collections.emptyMap();
    this.baseClassLoader = baseClassLoader;
    this.classLoader = createClassLoader();
  }

  private URLClassLoader createClassLoader() {
    URLClassLoader isolatedClassLoader;
    if (execJars.isEmpty()) {
      throw new RuntimeException("No Hive jars found for the specified version: " + version);
    }

    isolatedClassLoader =
        new URLClassLoader(version.name(), execJars.toArray(new URL[0]), null) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            Class<?> loaded = findLoadedClass(name);
            if (loaded == null) return doLoadClass(name, resolve);
            else return loaded;
          }

          private Class<?> doLoadClass(String name, boolean resolve) throws ClassNotFoundException {
            String classFileName = name.replace(".", "/") + ".class";
            if (isBarrierClass(name)) {
              try {
                LOG.debug("Classloader {} load barrier class {}", getName(), name);
                InputStream is = baseClassLoader.getResourceAsStream(classFileName);
                if (is == null) {
                  throw new ClassNotFoundException("Cannot load barrier class: " + name);
                }
                byte[] bytes = is.readAllBytes();
                return defineClass(name, bytes, 0, bytes.length);
              } catch (IOException e) {
                throw new ClassNotFoundException(name, e);
              }

            } else if (!isSharedClass(name)) {
              LOG.debug("Classloader {} load no shared class {}", getName(), name);
              return super.loadClass(name, resolve);
            } else {
              try {
                LOG.debug("Classloader {} load shared class {}", getName(), name);
                return baseClassLoader.loadClass(name);
              } catch (ClassNotFoundException e) {
                return super.loadClass(name, resolve);
              }
            }
          }
        };
    return isolatedClassLoader;
  }

  private boolean isSharedClass(String name) {
    boolean isHadoopClass =
        name.startsWith("org.apache.hadoop.") && !name.startsWith("org.apache.hadoop.hive.");
    return name.startsWith("org.slf4j")
        || name.startsWith("org.apache.log4j")
        || name.startsWith("org.apache.logging.log4j")
        || isHadoopClass
        || (name.startsWith("com.google") && !name.startsWith("com.google.cloud"))
        || name.startsWith("java.")
        || name.startsWith("javax.")
        || name.startsWith("com.sun.")
        || name.startsWith("org.ietf.jgss.")
        || name.startsWith(HiveClient.class.getName());
  }

  private boolean isBarrierClass(String name) {
    return name.startsWith(HiveClientImpl.class.getName())
        || name.startsWith(Shim.class.getName())
        || name.startsWith(HiveShimV2.class.getName())
        || name.startsWith(HiveShimV3.class.getName());
  }

  public HiveClient createClient(Properties properties) throws Exception {
    synchronized (this) {
      ClassLoader origLoader = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(classLoader);
      try {
        Class<?> clientClass =
            classLoader.loadClass("org.apache.hadoop.hive.metastore.HiveMetaStoreClient");
        Class<?> hiveConfClass = classLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf");
        Class<?> confClass = classLoader.loadClass("org.apache.hadoop.conf.Configuration");
        Object conf = confClass.getDeclaredConstructor().newInstance();
        Constructor<?> hiveConfCtor = hiveConfClass.getConstructor(confClass, Class.class);

        Object client = null;
        if (version == HiveClient.HiveVersion.HIVE2) {
          // Set any Hive2 specific configurations here
          Object hiveConfInstance = hiveConfCtor.newInstance(conf, hiveConfClass);
          Method setMethod = confClass.getMethod("set", String.class, String.class);
          for (Map.Entry<Object, Object> v : properties.entrySet()) {
            setMethod.invoke(hiveConfInstance, v.getKey(), v.getValue());
          }

          Constructor<?> ctor = clientClass.getConstructor(hiveConfClass);
          client = ctor.newInstance(hiveConfInstance);
        } else if (version == HiveClient.HiveVersion.HIVE3) {
          // Set any Hive3 specific configurations here
          Method setMethod = confClass.getMethod("set", String.class, String.class);
          for (Map.Entry<Object, Object> v : properties.entrySet()) {
            setMethod.invoke(conf, v.getKey(), v.getValue());
          }
          Constructor<?> ctor = clientClass.getConstructor(confClass);
          client = ctor.newInstance(conf);
        }

        return (HiveClient)
            classLoader
                .loadClass(HiveClientImpl.class.getName())
                .getConstructors()[0]
                .newInstance(version, client);
      } catch (InvocationTargetException e) {
        throw e;
      } finally {
        Thread.currentThread().setContextClassLoader(origLoader);
      }
    }
  }
}
