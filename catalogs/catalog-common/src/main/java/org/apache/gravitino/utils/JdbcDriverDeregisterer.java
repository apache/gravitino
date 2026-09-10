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

package org.apache.gravitino.utils;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Deregisters the JDBC drivers a catalog's ClassLoader registered with {@link DriverManager}.
 *
 * <p>This class is never called through its own name. {@link DriverManager} filters both {@code
 * getDrivers()} and {@code deregisterDriver()} by the class loader of the calling class, so a
 * driver defined by a catalog's isolated ClassLoader is invisible, and undeletable, from the server
 * ClassLoader. {@link ClassLoaderResourceCleanerUtils} therefore defines a copy of this class
 * inside the catalog's ClassLoader and invokes it reflectively, so that {@code DriverManager} sees
 * a caller that owns the drivers. Keep its dependencies to {@code java.*} only: the copy is defined
 * directly from bytecode and resolves everything through the catalog's ClassLoader.
 */
public final class JdbcDriverDeregisterer {

  private JdbcDriverDeregisterer() {}

  /**
   * Deregisters every driver defined by the ClassLoader of this class.
   *
   * @return the names of the drivers that were deregistered
   */
  public static List<String> deregisterAll() {
    ClassLoader owner = JdbcDriverDeregisterer.class.getClassLoader();
    List<String> deregistered = new ArrayList<>();
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      if (driver.getClass().getClassLoader() == owner) {
        try {
          DriverManager.deregisterDriver(driver);
          deregistered.add(driver.getClass().getName());
        } catch (Exception e) {
          // Leave the driver registered rather than failing the whole cleanup.
        }
      }
    }
    return deregistered;
  }
}
