/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gravitino.job;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobTemplateChange {

  @Test
  public void testRenameJobTemplate() {
    JobTemplateChange change = JobTemplateChange.rename("newTemplateName");
    Assertions.assertInstanceOf(JobTemplateChange.RenameJobTemplate.class, change);
    Assertions.assertEquals(
        "newTemplateName", ((JobTemplateChange.RenameJobTemplate) change).getNewName());

    JobTemplateChange change1 = JobTemplateChange.rename("newTemplateName");
    Assertions.assertEquals(change, change1);
    Assertions.assertEquals(change.hashCode(), change1.hashCode());
  }

  @Test
  public void testUpdateJobTemplateComment() {
    JobTemplateChange change = JobTemplateChange.updateComment("New comment");
    Assertions.assertInstanceOf(JobTemplateChange.UpdateJobTemplateComment.class, change);
    Assertions.assertEquals(
        "New comment", ((JobTemplateChange.UpdateJobTemplateComment) change).getNewComment());

    JobTemplateChange change1 = JobTemplateChange.updateComment("New comment");
    Assertions.assertEquals(change, change1);
    Assertions.assertEquals(change.hashCode(), change1.hashCode());
  }

  @Test
  public void testUpdateShellJobTemplate() {
    JobTemplateChange.ShellTemplateUpdate update =
        JobTemplateChange.ShellTemplateUpdate.builder()
            .withNewExecutable("newExecutable.sh")
            .withNewArguments(Lists.newArrayList("arg1", "arg2"))
            .withNewEnvironments(ImmutableMap.of("ENV1", "value1", "ENV2", "value2"))
            .withNewCustomFields(Collections.emptyMap())
            .withNewScripts(Lists.newArrayList("test1.sh", "test2.sh"))
            .build();

    JobTemplateChange change = JobTemplateChange.updateTemplate(update);

    Assertions.assertInstanceOf(JobTemplateChange.UpdateJobTemplate.class, change);
    JobTemplateChange.UpdateJobTemplate updateChange = (JobTemplateChange.UpdateJobTemplate) change;
    Assertions.assertEquals(
        update.getNewExecutable(), updateChange.getTemplateUpdate().getNewExecutable());
    Assertions.assertEquals(
        update.getNewArguments(), updateChange.getTemplateUpdate().getNewArguments());
    Assertions.assertEquals(
        update.getNewEnvironments(), updateChange.getTemplateUpdate().getNewEnvironments());
    Assertions.assertEquals(
        update.getNewCustomFields(), updateChange.getTemplateUpdate().getNewCustomFields());
    Assertions.assertEquals(
        update.getNewScripts(),
        ((JobTemplateChange.ShellTemplateUpdate) updateChange.getTemplateUpdate()).getNewScripts());
  }

  @Test
  public void testUpdateSparkJobTemplate() {
    JobTemplateChange.SparkTemplateUpdate update =
        JobTemplateChange.SparkTemplateUpdate.builder()
            .withNewClassName("org.apache.spark.examples.NewExample")
            .withNewExecutable("new-spark-examples.jar")
            .withNewArguments(Lists.newArrayList("arg1", "arg2"))
            .withNewEnvironments(ImmutableMap.of("ENV1", "value1", "ENV2", "value2"))
            .withNewCustomFields(Collections.emptyMap())
            .withNewJars(Lists.newArrayList("lib1.jar", "lib2.jar"))
            .withNewFiles(Lists.newArrayList("file1.txt", "file2.txt"))
            .withNewArchives(Lists.newArrayList("archive1.zip", "archive2.zip"))
            .withNewConfigs(ImmutableMap.of("spark.master", "local[*]", "spark.app.name", "NewApp"))
            .build();

    JobTemplateChange change = JobTemplateChange.updateTemplate(update);

    Assertions.assertInstanceOf(JobTemplateChange.UpdateJobTemplate.class, change);
    JobTemplateChange.UpdateJobTemplate updateChange = (JobTemplateChange.UpdateJobTemplate) change;
    Assertions.assertEquals(
        update.getNewClassName(),
        ((JobTemplateChange.SparkTemplateUpdate) updateChange.getTemplateUpdate())
            .getNewClassName());
    Assertions.assertEquals(
        update.getNewExecutable(), updateChange.getTemplateUpdate().getNewExecutable());
    Assertions.assertEquals(
        update.getNewArguments(), updateChange.getTemplateUpdate().getNewArguments());
    Assertions.assertEquals(
        update.getNewEnvironments(), updateChange.getTemplateUpdate().getNewEnvironments());
    Assertions.assertEquals(
        update.getNewCustomFields(), updateChange.getTemplateUpdate().getNewCustomFields());
    Assertions.assertEquals(
        update.getNewJars(),
        ((JobTemplateChange.SparkTemplateUpdate) updateChange.getTemplateUpdate()).getNewJars());
    Assertions.assertEquals(
        update.getNewFiles(),
        ((JobTemplateChange.SparkTemplateUpdate) updateChange.getTemplateUpdate()).getNewFiles());
    Assertions.assertEquals(
        update.getNewArchives(),
        ((JobTemplateChange.SparkTemplateUpdate) updateChange.getTemplateUpdate())
            .getNewArchives());
    Assertions.assertEquals(
        update.getNewConfigs(),
        ((JobTemplateChange.SparkTemplateUpdate) updateChange.getTemplateUpdate()).getNewConfigs());
  }
}
