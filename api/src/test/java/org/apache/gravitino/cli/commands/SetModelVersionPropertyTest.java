package org.apache.gravitino.cli.commands;

import org.apache.gravitino.cli.CommandContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SetModelVersionPropertyTest {

    @BeforeAll
    static void setup() {
        Main.useExit = false;
    }

    @Test
    void validateBothAliasAndVersion() {
        CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
        CommandContext context = new CommandContext(mockCmdLine);

        SetModelVersionProperty command =
                new SetModelVersionProperty(
                        context,
                        "metalake1",
                        "catalog1",
                        "schema1",
                        "model1",
                        1, // version
                        "alias1", // alias
                        "prop",
                        "value");

        Assertions.assertThrows(RuntimeException.class, command::validate);
    }

    @Test
    void validateNeitherAliasNorVersion() {
        CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
        CommandContext context = new CommandContext(mockCmdLine);

        SetModelVersionProperty command =
                new SetModelVersionProperty(
                        context,
                        "metalake1",
                        "catalog1",
                        "schema1",
                        "model1",
                        null, // no version
                        null, // no alias
                        "prop",
                        "value");

        Assertions.assertThrows(RuntimeException.class, command::validate);
    }

    @Test
    void validateOnlyAliasProvided() {
        CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
        CommandContext context = new CommandContext(mockCmdLine);

        SetModelVersionProperty command =
                new SetModelVersionProperty(
                        context,
                        "metalake1",
                        "catalog1",
                        "schema1",
                        "model1",
                        null, // no version
                        "alias1", // only alias
                        "prop",
                        "value");

        Assertions.assertDoesNotThrow(command::validate);
    }

    @Test
    void validateOnlyVersionProvided() {
        CommandLine mockCmdLine = Mockito.mock(CommandLine.class);
        CommandContext context = new CommandContext(mockCmdLine);

        SetModelVersionProperty command =
                new SetModelVersionProperty(
                        context,
                        "metalake1",
                        "catalog1",
                        "schema1",
                        "model1",
                        1, // only version
                        null,
                        "prop",
                        "value");

        Assertions.assertDoesNotThrow(command::validate);
    }
}