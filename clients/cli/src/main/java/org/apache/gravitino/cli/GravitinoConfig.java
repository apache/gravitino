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

public class GravitinoConfig {

  private static String defaultFile = ".gravitino";

  private String configFile;
  private String metalake;
  private String url;

  public GravitinoConfig(String file) {
    if (file == null) {
      configFile = System.getProperty("user.home") + "/" + defaultFile;
    } else {
      configFile = file;
    }
  }

  public boolean fileExists() {
    File file = new File(configFile);
    return file.exists();
  }

  public void read() {
    String metalakeKey = "metalake";
    String urlKey = "URL";
    Properties prop = new Properties();

    try (FileInputStream stream = new FileInputStream(configFile)) {
      prop.load(stream);
    } catch (FileNotFoundException ex) {
      // ignore as config file is optional
      return;
    } catch (IOException exp) {
      System.err.println(exp.getMessage());
    }

    if (prop.containsKey(metalakeKey)) {
      metalake = prop.getProperty(metalakeKey);
    }
    if (prop.containsKey(urlKey)) {
      url = prop.getProperty(urlKey);
    }
  }

  public String getMetalakeName() {
    return metalake;
  }

  public String getGravitinoURL() {
    return url;
  }

  public String getConfigFile() {
    return configFile;
  }
}
