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
package org.apache.gravitino.catalog.mysql;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.jdbc.NonRegisteringDriver;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.utils.JdbcUrlUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestMysqlJdbcUrlCredentials {
  @ParameterizedTest
  @ValueSource(
      strings = {"jdbc:mysql+srv:", "jdbc:mysql+srv:loadbalance:", "jdbc:mysql+srv:replication:"})
  void testSrvPublicOptionsRemainUsableWithoutAllowingPasswords(String scheme) throws SQLException {
    String url =
        scheme + "//host/db?%70asswordCharacterEncoding=UTF-8&disconnectOnExpiredPasswords=false";
    // Parse without resolving DNS SRV records or opening a connection.
    Assertions.assertTrue(new NonRegisteringDriver().acceptsURL(url));
    Map<String, String> properties = ConnectionUrlParser.parseConnectionString(url).getProperties();
    Assertions.assertEquals("UTF-8", properties.get("passwordCharacterEncoding"));
    Assertions.assertEquals("false", properties.get("disconnectOnExpiredPasswords"));
    Assertions.assertFalse(JdbcUrlUtils.containsCredentials(url));
    Assertions.assertEquals(url, new JdbcConfig(Map.of("jdbc-url", url)).getJdbcUrl());

    String passwordUrl = url + "&password=hidden";
    Assertions.assertEquals(
        "hidden",
        ConnectionUrlParser.parseConnectionString(passwordUrl).getProperties().get("password"));
    Assertions.assertTrue(JdbcUrlUtils.containsCredentials(passwordUrl));
    JdbcConfig config = new JdbcConfig(Map.of("jdbc-url", passwordUrl));
    Assertions.assertThrows(IllegalArgumentException.class, config::getJdbcUrl);
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "jdbc:mysql://user:pass%2Fword@host/db|pass/word",
        "jdbc:mysql://user:pass%3Fword@host/db|pass?word",
        "jdbc:mysql://user:pass%23word@host/db|pass#word",
        "jdbc:mysql://user:pass%252Fword@host/db|pass%2Fword",
        "jdbc:mysql:loadbalance://user:pass%2Fword@host1,host2/db|pass/word"
      },
      delimiter = '|')
  void testDriverRecognizesEncodedUserInfoThatGravitinoRejects(String url, String password) {
    // Parse with the pinned Connector/J without opening a network connection.
    ConnectionUrl parsed = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    Assertions.assertEquals("user", parsed.getMainHost().getUser());
    Assertions.assertEquals(password, parsed.getMainHost().getPassword());
    Assertions.assertTrue(JdbcUrlUtils.containsCredentials(url));
    JdbcConfig config = new JdbcConfig(Map.of("jdbc-url", url));
    IllegalArgumentException error =
        Assertions.assertThrows(IllegalArgumentException.class, config::getJdbcUrl);
    Assertions.assertFalse(error.getMessage().contains(password));
    Assertions.assertFalse(error.getMessage().contains(url));
  }
}
