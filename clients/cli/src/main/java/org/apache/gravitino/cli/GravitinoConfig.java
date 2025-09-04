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

package org.apache.gravitino.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * GravitinoConfig is a configuration class used to read and manage Gravitino settings. It supports
 * setting a configuration file, reading properties like 'metalake' and 'URL', and verifying if the
 * config file exists.
 */
public class GravitinoConfig {
  private static String defaultFile = ".gravitino";
  private final String configFile;
  private String metalake;
  private String url;
  private boolean ignore;
  private String authType;
  private OAuthData oauth;
  private KerberosData kerberos;

  /**
   * Creates a GravitinoConfig object with a specified config file. If no file is provided, it
   * defaults to `.gravitino` the user's home directory.
   *
   * @param file The path to the config file or null to use the default.
   */
  public GravitinoConfig(String file) {
    if (file == null) {
      configFile = System.getProperty("user.home") + "/" + defaultFile;
    } else {
      configFile = file;
    }
  }

  /**
   * Checks if the configuration file exists.
   *
   * @return true if the config file exists, false otherwise.
   */
  public boolean fileExists() {
    File file = new File(configFile);
    return file.exists();
  }

  /**
   * Reads the configuration file and loads the 'metalake', 'URL' and other properties. If the file
   * is not found, it is ignored as the config file is optional.
   */
  public void read() {
    String metalakeKey = "metalake";
    String urlKey = "URL";
    String ignoreKey = "ignore";
    String authKey = "auth";
    Properties prop = new Properties();

    try (FileInputStream stream = new FileInputStream(configFile)) {
      prop.load(stream);
    } catch (FileNotFoundException ex) {
      // ignore as config file is optional
      return;
    } catch (IOException exp) {
      System.err.println(exp.getMessage());
      Main.exit(-1);
    }

    if (prop.containsKey(metalakeKey)) {
      metalake = prop.getProperty(metalakeKey);
    }
    if (prop.containsKey(urlKey)) {
      url = prop.getProperty(urlKey);
    }
    if (prop.containsKey(ignoreKey)) {
      ignore = prop.getProperty(ignoreKey).equals("true");
    }
    if (prop.containsKey(authKey)) {
      authType = prop.getProperty(authKey);
    }

    if ("oauth".equals(authType)) {
      oauth =
          new OAuthData(
              prop.getProperty("serverURI"),
              prop.getProperty("credential"),
              prop.getProperty("token"),
              prop.getProperty("scope"));
    } else if ("kerberos".equals(authType)) {
      kerberos = new KerberosData(prop.getProperty("principal"), prop.getProperty("keytabFile"));
    }
  }

  /**
   * Retrieves the metalake name stored in the configuration.
   *
   * @return The metalake name or null if not set.
   */
  public String getMetalakeName() {
    return metalake;
  }

  /**
   * Retrieves the Gravitino URL stored in the configuration.
   *
   * @return The Gravitino URL or null if not set.
   */
  public String getGravitinoURL() {
    return url;
  }

  /**
   * Retrieves the ignore options stored in the configuration.
   *
   * @return The config file path.
   */
  public boolean getIgnore() {
    return ignore;
  }

  /**
   * Retrieves the path to the configuration file being used.
   *
   * @return The config file path.
   */
  public String getConfigFile() {
    return configFile;
  }

  /**
   * Retrieves the Gravitino authentication type stored in the configuration.
   *
   * @return The Gravitino authentication type or null if not set.
   */
  public String getGravitinoAuthType() {
    return authType;
  }

  /**
   * Retrieves the Gravitino OAuth configuration.
   *
   * @return The Gravitino OAuth data or null if not set.
   */
  public OAuthData getOAuth() {
    return oauth;
  }

  /**
   * Retrieves the Gravitino kerberos configuration.
   *
   * @return The Gravitino Kerberos data or null if not set.
   */
  public KerberosData getKerberos() {
    return kerberos;
  }
}
