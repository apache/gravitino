package com.datastrato.gravitino.integration.test.web.rest;

import com.datastrato.gravitino.client.GravitinoVersion;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.CommandExecutor;
import com.datastrato.gravitino.integration.test.util.ProcessData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AuthenticationOperationsIT extends AbstractIT {

    @BeforeAll
    public static void initTestEnv() throws Exception {
    }


    @Test
    public void testAuthenticationApi() {
        Object ret =
                CommandExecutor.executeCommandLocalHost(
                        "git rev-parse HEAD", false, ProcessData.TypesOfData.OUTPUT);
        String gitCommitId = ret.toString().replace("\n", "");

        GravitinoVersion gravitinoVersion = client.getVersion();
        Assertions.assertEquals(System.getenv("PROJECT_VERSION"), gravitinoVersion.version());
        Assertions.assertFalse(gravitinoVersion.compileDate().isEmpty());
        Assertions.assertEquals(gitCommitId, gravitinoVersion.gitCommit());
    }
}
