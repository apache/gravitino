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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.utils.FullNameUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFullNameUtil {
  private Options options;

  @BeforeEach
  public void setUp() {
    Main.useExit = false;
    options = new GravitinoOptions().options();
  }

  @Test
  void testToModel() throws ParseException {
    String[] args = {"table", "list", "-i", "--name", "Model_catalog.schema.model"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    NameIdentifier modelIdent = FullNameUtil.toModel(fullName);
    Assertions.assertEquals("model", modelIdent.name());
    Assertions.assertArrayEquals(new String[] {"schema"}, modelIdent.namespace().levels());
  }

  @Test
  void testToTable() throws ParseException {
    String[] args = {"table", "list", "-i", "--name", "Table_catalog.schema.table"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    NameIdentifier tableIdent = FullNameUtil.toTable(fullName);
    Assertions.assertEquals("table", tableIdent.name());
    Assertions.assertArrayEquals(new String[] {"schema"}, tableIdent.namespace().levels());
  }

  @Test
  void testToFileset() throws ParseException {
    String[] args = {"fileset", "list", "-i", "--name", "Fileset_catalog.schema.fileset"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    NameIdentifier filesetIdent = FullNameUtil.toFileset(fullName);
    Assertions.assertEquals("fileset", filesetIdent.name());
    Assertions.assertArrayEquals(new String[] {"schema"}, filesetIdent.namespace().levels());
  }

  @Test
  void testToTopic() throws ParseException {
    String[] args = {"topic", "list", "-i", "--name", "Topic_catalog.schema.topic"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    NameIdentifier topicIdent = FullNameUtil.toTopic(fullName);
    Assertions.assertEquals("topic", topicIdent.name());
    Assertions.assertArrayEquals(new String[] {"schema"}, topicIdent.namespace().levels());
  }
}
