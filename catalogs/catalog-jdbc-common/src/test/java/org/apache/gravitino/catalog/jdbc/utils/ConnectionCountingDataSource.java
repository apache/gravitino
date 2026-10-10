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
package org.apache.gravitino.catalog.jdbc.utils;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

/**
 * A test {@link DataSource} whose connections only record the SQL they execute. It tracks how many
 * connections are borrowed at once, so tests can assert that an operation never holds one
 * connection while borrowing another.
 */
public class ConnectionCountingDataSource {

  private final AtomicInteger borrowed = new AtomicInteger();
  private final AtomicInteger peakBorrowed = new AtomicInteger();
  private final AtomicInteger totalBorrows = new AtomicInteger();
  private final List<String> executedSql = new CopyOnWriteArrayList<>();
  private final DataSource dataSource =
      (DataSource)
          Proxy.newProxyInstance(
              DataSource.class.getClassLoader(),
              new Class<?>[] {DataSource.class},
              (proxy, method, args) -> {
                if ("getConnection".equals(method.getName())) {
                  return borrow();
                }
                return objectMethod(proxy, method.getName(), args, "DataSource");
              });

  /** Returns the data source to pass to the operations under test. */
  public DataSource dataSource() {
    return dataSource;
  }

  /** Returns the number of connections currently borrowed and not yet closed. */
  public int borrowed() {
    return borrowed.get();
  }

  /** Returns the largest number of connections that were borrowed at the same time. */
  public int peakBorrowed() {
    return peakBorrowed.get();
  }

  /** Returns how many connections were borrowed in total. */
  public int totalBorrows() {
    return totalBorrows.get();
  }

  /** Returns the update statements executed so far, in order. */
  public List<String> executedSql() {
    return executedSql;
  }

  private Connection borrow() {
    totalBorrows.incrementAndGet();
    peakBorrowed.accumulateAndGet(borrowed.incrementAndGet(), Math::max);
    AtomicBoolean closed = new AtomicBoolean();
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "close":
                  if (closed.compareAndSet(false, true)) {
                    borrowed.decrementAndGet();
                  }
                  return null;
                case "setCatalog":
                case "setSchema":
                  return null;
                case "createStatement":
                  return statement();
                case "getMetaData":
                  return metaData();
                default:
                  return objectMethod(proxy, method.getName(), args, "Connection");
              }
            });
  }

  // Answers Object methods so the proxies can be printed or compared, and rejects anything else.
  private static Object objectMethod(Object proxy, String name, Object[] args, String type) {
    switch (name) {
      case "toString":
        return "Counting" + type + "@" + Integer.toHexString(System.identityHashCode(proxy));
      case "hashCode":
        return System.identityHashCode(proxy);
      case "equals":
        return proxy == args[0];
      default:
        throw new UnsupportedOperationException(type + "." + name);
    }
  }

  // Reports a null driver version, which the JDBC contract allows.
  private static DatabaseMetaData metaData() {
    return (DatabaseMetaData)
        Proxy.newProxyInstance(
            DatabaseMetaData.class.getClassLoader(),
            new Class<?>[] {DatabaseMetaData.class},
            (proxy, method, args) -> {
              if ("getDriverVersion".equals(method.getName())) {
                return null;
              }
              return objectMethod(proxy, method.getName(), args, "DatabaseMetaData");
            });
  }

  private Statement statement() {
    return (Statement)
        Proxy.newProxyInstance(
            Statement.class.getClassLoader(),
            new Class<?>[] {Statement.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "executeUpdate":
                  executedSql.add((String) args[0]);
                  return 0;
                case "close":
                  return null;
                default:
                  return objectMethod(proxy, method.getName(), args, "Statement");
              }
            });
  }
}
