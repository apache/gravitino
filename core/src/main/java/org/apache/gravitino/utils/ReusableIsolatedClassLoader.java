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

package org.apache.gravitino.utils;

import com.google.common.base.Objects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReusableIsolatedClassLoader extends IsolatedClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ReusableIsolatedClassLoader.class);

  private final Map<String, String> conf;

  // Reference count to track the number of users of this class loader.
  private AtomicInteger refCount = new AtomicInteger(0);

  // Last access time in milliseconds. This value is used to determine when to clean up the class
  // loader when refCount drops to zero.
  private long lastAccessTime = Long.MAX_VALUE;

  public static final BiMap<ClassLoaderCacheKey, IsolatedClassLoader> CLASSLOADER_CACHE =
      Maps.synchronizedBiMap(HashBiMap.create());

  public static final ScheduledThreadPoolExecutor CLASS_LOADER_CLEANER =
      new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory());

  static {
    CLASS_LOADER_CLEANER.scheduleAtFixedRate(
        () -> {
          synchronized (CLASSLOADER_CACHE) {
            if (CLASSLOADER_CACHE.isEmpty()) {
              return;
            }

            Iterator<Entry<ClassLoaderCacheKey, IsolatedClassLoader>> iterator =
                CLASSLOADER_CACHE.entrySet().iterator();
            while (iterator.hasNext()) {
              Entry<ClassLoaderCacheKey, IsolatedClassLoader> entry = iterator.next();
              IsolatedClassLoader classLoader = entry.getValue();

              if (classLoader instanceof ReusableIsolatedClassLoader) {
                ReusableIsolatedClassLoader reusableIsolatedClassLoader =
                    (ReusableIsolatedClassLoader) classLoader;

                // If the class loader is not used for more than 60 seconds, we clean it up.
                // TODO make 60000 configurable.
                if (reusableIsolatedClassLoader.getRefCount().get() <= 0
                    && (System.currentTimeMillis() - reusableIsolatedClassLoader.lastAccessTime)
                        > 60000) {
                  iterator.remove();
                  ClassLoaderResourceCleanerUtils.closeClassLoaderResource(
                      reusableIsolatedClassLoader.classLoader, reusableIsolatedClassLoader.conf);
                  classLoader.close();
                  classLoader.classLoader = null;
                  LOG.info("Cleaned up classloader for key: {}", entry.getKey());
                }
              }
            }
          }
        },
        10,
        10,
        TimeUnit.SECONDS);
  }

  public ReusableIsolatedClassLoader(
      List<URL> execJars,
      List<String> sharedClasses,
      List<String> barrierClasses,
      Map<String, String> conf) {
    super(execJars, sharedClasses, barrierClasses);
    this.conf = conf;
  }

  public static class ClassLoaderCacheKey {
    private final String provider;
    private final String packagePath;
    private final Map<String, String> options;

    // Configurable that can decide which class loader to use. For example, we may
    // want to use different class loader for different authentication methods, then
    // we can add "auth.method" to the useful options. Currently, only `provider` and `package`
    // are used, please refer to `ClassLoaderCacheKey`.
    // Note: We need to make sure the options here are not too many, otherwise, it will cause
    // too much compute overhead when creating the key.
    private static final Set<String> CLASS_LOADER_AFFECT_OPTIONS = Set.of();

    private Map<String, String> extractUsefulOptions(Map<String, String> options) {
      return options.entrySet().stream()
          .filter(e -> CLASS_LOADER_AFFECT_OPTIONS.contains(e.getKey()))
          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private ClassLoaderCacheKey(String provider, String packagePath, Map<String, String> options) {
      this.provider = provider;
      this.packagePath = packagePath;
      this.options = extractUsefulOptions(options);
    }

    public static ClassLoaderCacheKey of(
        String provider, String packagePath, Map<String, String> options) {
      return new ClassLoaderCacheKey(provider, packagePath, options);
    }

    public static ClassLoaderCacheKey of(String provider, String packagePath) {
      return new ClassLoaderCacheKey(provider, packagePath, Collections.emptyMap());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ClassLoaderCacheKey)) {
        return false;
      }

      ClassLoaderCacheKey that = (ClassLoaderCacheKey) o;

      return Objects.equal(provider, that.provider)
          && Objects.equal(packagePath, that.packagePath)
          && Objects.equal(options, that.options);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(provider, packagePath, options);
    }
  }

  @Nonnull
  public AtomicInteger getRefCount() {
    return refCount;
  }

  public void markAccess() {
    refCount.incrementAndGet();
    lastAccessTime = Long.MAX_VALUE;
  }

  @Override
  public void close() {
    try {
      if (classLoader != null) {
        // We will not close the class loader immediately as it may be used by later calls. If
        // the class loader is not used for more than 60 seconds, we will clean it up in the
        // background.
        if (refCount.decrementAndGet() <= 0) {
          lastAccessTime = System.currentTimeMillis();
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to close classloader", e);
    }
  }

  private static ThreadFactory newDaemonThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("IsolateClassloader-cleaner" + "-%d")
        .build();
  }

  public static ReusableIsolatedClassLoader buildClassLoader(
      List<String> libAndResourcesPaths, Map<String, String> conf) {
    // Listing all the classPath under the package path and build the isolated class loader.
    List<URL> classPathContents = Lists.newArrayList();
    for (String path : libAndResourcesPaths) {
      File folder = new File(path);
      if (!folder.exists() || !folder.isDirectory() || !folder.canRead() || !folder.canExecute()) {
        throw new IllegalArgumentException(
            String.format("Invalid package path: %s in %s", path, libAndResourcesPaths));
      }

      // Add all the jar under the folder to classpath.
      Arrays.stream(folder.listFiles())
          .filter(f -> f.getName().endsWith(".jar"))
          .forEach(
              f -> {
                try {
                  classPathContents.add(f.toURI().toURL());
                } catch (MalformedURLException e) {
                  LOG.warn("Failed to read jar file: {}", f.getAbsolutePath(), e);
                }
              });

      // Add itself to the classpath.
      try {
        classPathContents.add(folder.toURI().toURL());
      } catch (MalformedURLException e) {
        LOG.warn("Failed to read directory: {}", folder.getAbsolutePath(), e);
      }
    }

    return new ReusableIsolatedClassLoader(
        classPathContents, Collections.emptyList(), Collections.emptyList(), conf);
  }
}
