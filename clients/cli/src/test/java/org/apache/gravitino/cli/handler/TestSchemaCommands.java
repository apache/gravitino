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

package org.apache.gravitino.cli.handler;

import org.apache.gravitino.cli.CommandActions;
import org.apache.gravitino.cli.CommandEntities;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.MainCli;
import org.apache.gravitino.cli.outputs.OutputFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class TestSchemaCommand {

  @Test
  void testHelpCommandOfSchema() {
    String[] args1 = {"schema", "--help"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(args1));

    String[] args2 = {"schema", "-h"};
    Assertions.assertDoesNotThrow(() -> MainCli.main(args2));
  }

  @Test
  void testSchemaCommand() {
    String[] detailsArgs = {
      CommandEntities.SCHEMA,
      CommandActions.DETAILS,
      GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.HELP
    };
    Assertions.assertDoesNotThrow(() -> MainCli.main(detailsArgs));

    String[] createArgs = {
      CommandEntities.SCHEMA,
      CommandActions.CREATE,
      GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.HELP
    };
    Assertions.assertDoesNotThrow(() -> MainCli.main(createArgs));

    String[] deleteArgs = {
      CommandEntities.SCHEMA,
      CommandActions.DELETE,
      GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.HELP
    };
    Assertions.assertDoesNotThrow(() -> MainCli.main(deleteArgs));

    String[] setArgs = {
      CommandEntities.SCHEMA,
      CommandActions.SET,
      GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.HELP
    };
    Assertions.assertDoesNotThrow(() -> MainCli.main(setArgs));

    String[] reomveArgs = {
      CommandEntities.SCHEMA,
      CommandActions.REMOVE,
      GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.HELP
    };
    Assertions.assertDoesNotThrow(() -> MainCli.main(reomveArgs));

    String[] properties = {
      CommandEntities.SCHEMA,
      CommandActions.PROPERTIES,
      GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.HELP
    };
    Assertions.assertDoesNotThrow(() -> MainCli.main(properties));

    String[] listArgs = {
      CommandEntities.SCHEMA,
      CommandActions.LIST,
      GravitinoOptions.OPTION_LONG_PREFIX + GravitinoOptions.HELP
    };
    Assertions.assertDoesNotThrow(() -> MainCli.main(listArgs));
  }

  @Test
  void testGetDetailsOfSchema() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaDetails mockSchemaDetails =
        new org.apache.gravitino.cli.handler.MockSchemaDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockSchemaDetails);
    String[] args = {
      "mock_details", "--metalake", "ml1", "--name", "catalog1.schema1", "--no-audit"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaDetails.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockSchemaDetails.commonOptions.name);
    Assertions.assertFalse(mockSchemaDetails.audit);
  }

  @Test
  void testGetAuditOfSchema() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaDetails mockSchemaDetails =
        new org.apache.gravitino.cli.handler.MockSchemaDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockSchemaDetails);
    String[] args = {"mock_details", "--metalake", "ml1", "--name", "catalog1.schema1", "--audit"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaDetails.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockSchemaDetails.commonOptions.name);
    Assertions.assertTrue(mockSchemaDetails.audit);
  }

  @Test
  void testGetDetailsOfSchemaWithDefaults() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaDetails mockSchemaDetails =
        new org.apache.gravitino.cli.handler.MockSchemaDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockSchemaDetails);
    String[] args = {"mock_details", "--name", "catalog1.schema1", "--metalake", "ml1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaDetails.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockSchemaDetails.commonOptions.name);
    Assertions.assertFalse(mockSchemaDetails.audit);
  }

  @Test
  void testGetDetailsOfSchemaWithMalformedEntity() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaDetails mockSchemaDetails =
        new org.apache.gravitino.cli.handler.MockSchemaDetails();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_details", mockSchemaDetails);
    String[] args = {"mock_details", "--metalake", "ml1", "--name", "catalog1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testCreateSchema() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaCreate mockSchemaCreate =
        new org.apache.gravitino.cli.handler.MockSchemaCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockSchemaCreate);
    String[] args = {
      "mock_create",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1",
      "--comment",
      "schema1 comment"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaCreate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockSchemaCreate.commonOptions.name);
    Assertions.assertEquals("schema1 comment", mockSchemaCreate.comment);
  }

  @Test
  void testCreateSchemaWithDefaults() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaCreate mockSchemaCreate =
        new org.apache.gravitino.cli.handler.MockSchemaCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockSchemaCreate);
    String[] args = {"mock_create", "--name", "catalog1.schema1", "--metalake", "ml1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaCreate.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockSchemaCreate.commonOptions.name);
    Assertions.assertEquals("", mockSchemaCreate.comment);
  }

  @Test
  void testCreateSchemaWithMalformedEntity() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaCreate mockSchemaCreate =
        new org.apache.gravitino.cli.handler.MockSchemaCreate();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_create", mockSchemaCreate);
    String[] args = {"mock_create", "--metalake", "ml1", "--name", "catalog1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testDeleteSchema() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaDelete mockSchemaDelete =
        new org.apache.gravitino.cli.handler.MockSchemaDelete();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_delete", mockSchemaDelete);
    String[] args = {"mock_delete", "--metalake", "ml1", "--name", "catalog1.schema1", "--force"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaDelete.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockSchemaDelete.commonOptions.name);
    Assertions.assertTrue(mockSchemaDelete.force);
  }

  @Test
  void testDeleteSchemaWithDefaults() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaDelete mockSchemaDelete =
        new org.apache.gravitino.cli.handler.MockSchemaDelete();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_delete", mockSchemaDelete);
    String[] args = {"mock_delete", "--name", "catalog1.schema1", "--metalake", "ml1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaDelete.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockSchemaDelete.commonOptions.name);
    Assertions.assertFalse(mockSchemaDelete.force);
  }

  @Test
  void testDeleteSchemaWithMalformedEntity() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaDelete mockSchemaDelete =
        new org.apache.gravitino.cli.handler.MockSchemaDelete();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_delete", mockSchemaDelete);
    String[] args = {"mock_delete", "--metalake", "ml1", "--name", "catalog1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testSetSchemaProperty() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaSet mockSchemaSet =
        new org.apache.gravitino.cli.handler.MockSchemaSet();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_set", mockSchemaSet);
    String[] args = {
      "mock_set",
      "--metalake",
      "ml1",
      "--name",
      "catalog1.schema1",
      "--property",
      "key1",
      "--value",
      "value1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaSet.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockSchemaSet.commonOptions.name);
    Assertions.assertEquals("key1", mockSchemaSet.propertyOptions.property);
    Assertions.assertEquals("value1", mockSchemaSet.propertyOptions.value);
  }

  @Test
  void testSetSchemaPropertyWithoutValue() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaSet mockSchemaSet =
        new org.apache.gravitino.cli.handler.MockSchemaSet();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_set", mockSchemaSet);
    String[] args = {
      "mock_set", "--metalake", "ml1", "--name", "catalog1.schema1", "--property", "key1"
    };

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testSetSchemaPropertyWithoutProperty() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaSet mockSchemaSet =
        new org.apache.gravitino.cli.handler.MockSchemaSet();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_set", mockSchemaSet);
    String[] args = {
      "mock_set", "--metalake", "ml1", "--name", "catalog1.schema1", "--value", "value1"
    };

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testSetSchemaPropertyWithMalformedEntity() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaSet mockSchemaSet =
        new org.apache.gravitino.cli.handler.MockSchemaSet();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_set", mockSchemaSet);
    String[] args = {
      "mock_set",
      "--metalake",
      "ml1",
      "--name",
      "catalog1",
      "--property",
      "key1",
      "--value",
      "value1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testRemoveSchemaProperty() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaRemove mockSchemaRemove =
        new org.apache.gravitino.cli.handler.MockSchemaRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockSchemaRemove);
    String[] args = {
      "mock_remove", "--metalake", "ml1", "--name", "catalog1.schema1", "--property", "key1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaRemove.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockSchemaRemove.commonOptions.name);
    Assertions.assertEquals("key1", mockSchemaRemove.property);
  }

  @Test
  void testRemoveSchemaPropertyWithoutProperty() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaRemove mockSchemaRemove =
        new org.apache.gravitino.cli.handler.MockSchemaRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockSchemaRemove);
    String[] args = {"mock_remove", "--metalake", "ml1", "--name", "catalog1.schema1"};

    Assertions.assertThrowsExactly(
        picocli.CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }

  @Test
  void testRemoveSchemaPropertyWithMalformedEntity() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaRemove mockSchemaRemove =
        new org.apache.gravitino.cli.handler.MockSchemaRemove();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_remove", mockSchemaRemove);
    String[] args = {
      "mock_remove", "--metalake", "ml1", "--name", "catalog1", "--property", "key1"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testListSchemaProperties() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaProperties mockSchemaProperties =
        new org.apache.gravitino.cli.handler.MockSchemaProperties();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_properties", mockSchemaProperties);
    String[] args = {
      "mock_properties", "--metalake", "ml1", "--name", "catalog1.schema1", "--output", "TABLE"
    };

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaProperties.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockSchemaProperties.commonOptions.name);
    Assertions.assertEquals(
        OutputFormat.OutputType.TABLE, mockSchemaProperties.commonOptions.outputFormat);
  }

  @Test
  void testListSchemaPropertiesWithDefaults() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaProperties mockSchemaProperties =
        new org.apache.gravitino.cli.handler.MockSchemaProperties();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_properties", mockSchemaProperties);
    String[] args = {"mock_properties", "--metalake", "ml1", "--name", "catalog1.schema1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaProperties.commonOptions.metalake);
    Assertions.assertEquals("catalog1.schema1", mockSchemaProperties.commonOptions.name);
    Assertions.assertEquals(
        OutputFormat.OutputType.PLAIN, mockSchemaProperties.commonOptions.outputFormat);
  }

  @Test
  void testListSchemaPropertiesWithMalformedEntity() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaProperties mockSchemaProperties =
        new org.apache.gravitino.cli.handler.MockSchemaProperties();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_properties", mockSchemaProperties);
    String[] args = {"mock_properties", "--metalake", "ml1", "--name", "catalog1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(1, exitCode);
  }

  @Test
  void testListSchema() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaList mockSchemaList =
        new org.apache.gravitino.cli.handler.MockSchemaList();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_list", mockSchemaList);
    String[] args = {"mock_list", "--metalake", "ml1", "--name", "catalog1", "--output", "TABLE"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaList.commonOptions.metalake);
    Assertions.assertEquals("catalog1", mockSchemaList.commonOptions.name);
    Assertions.assertEquals(
        OutputFormat.OutputType.TABLE, mockSchemaList.commonOptions.outputFormat);
  }

  @Test
  void testListSchemaWithDefaults() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaList mockSchemaList =
        new org.apache.gravitino.cli.handler.MockSchemaList();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_list", mockSchemaList);
    String[] args = {"mock_list", "--metalake", "ml1", "--name", "catalog1"};

    int exitCode = cmd.execute(args);
    Assertions.assertEquals(0, exitCode);
    Assertions.assertEquals("ml1", mockSchemaList.commonOptions.metalake);
    Assertions.assertEquals("catalog1", mockSchemaList.commonOptions.name);
    Assertions.assertEquals(
        OutputFormat.OutputType.PLAIN, mockSchemaList.commonOptions.outputFormat);
  }

  @Test
  void testListSchemaWithMalformedEntity() {
    SchemaCliHandler root = new SchemaCliHandler();
    org.apache.gravitino.cli.handler.MockSchemaList mockSchemaList =
        new org.apache.gravitino.cli.handler.MockSchemaList();

    picocli.CommandLine cmd =
        new picocli.CommandLine(root).addSubcommand("mock_list", mockSchemaList);
    String[] args = {"mock_list", "--metalake", "ml1"};

    Assertions.assertThrowsExactly(
        CommandLine.MissingParameterException.class, () -> cmd.parseArgs(args));
  }
}

/** mock class for SchemaDetails */
class MockSchemaDetails extends SchemaDetails {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** mock class for SchemaCreate */
class MockSchemaCreate extends SchemaCreate {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** mock class for SchemaDelete */
class MockSchemaDelete extends SchemaDelete {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** mock class for Schema Set */
class MockSchemaSet extends SchemaSet {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** mock class for Schema Properties */
class MockSchemaRemove extends SchemaRemove {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** mock class for Schema Properties */
class MockSchemaProperties extends SchemaProperties {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}

/** mock class for Schema List */
class MockSchemaList extends SchemaList {
  @Override
  protected Integer doCall() throws Exception {
    return 0;
  }
}
