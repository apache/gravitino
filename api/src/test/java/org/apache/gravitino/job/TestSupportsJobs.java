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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSupportsJobs {

  /**
   * Mimics a {@link SupportsJobs} implementor written before the {@code includeOutput} overload was
   * introduced - it only implements {@link #getJob(String)}, the sole abstract method both before
   * and after that change, so it must keep compiling and behaving correctly without any
   * modifications.
   */
  private static class LegacyJobsImpl implements SupportsJobs {

    private final JobHandle handle;

    LegacyJobsImpl(JobHandle handle) {
      this.handle = handle;
    }

    @Override
    public List<JobTemplate> listJobTemplates() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerJobTemplate(JobTemplate jobTemplate)
        throws JobTemplateAlreadyExistsException {
      throw new UnsupportedOperationException();
    }

    @Override
    public JobTemplate getJobTemplate(String jobTemplateName) throws NoSuchJobTemplateException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean deleteJobTemplate(String jobTemplateName) throws InUseException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<JobHandle> listJobs(String jobTemplateName) throws NoSuchJobTemplateException {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<JobHandle> listJobs() {
      throw new UnsupportedOperationException();
    }

    @Override
    public JobHandle runJob(String jobTemplateName, Map<String, String> jobConf)
        throws NoSuchJobTemplateException {
      throw new UnsupportedOperationException();
    }

    @Override
    public JobHandle getJob(String jobId) throws NoSuchJobException {
      return handle;
    }

    @Override
    public JobHandle cancelJob(String jobId) throws NoSuchJobException {
      throw new UnsupportedOperationException();
    }
  }

  private static class FakeJobHandle implements JobHandle {
    @Override
    public String jobTemplateName() {
      return "template";
    }

    @Override
    public String jobId() {
      return "job-1";
    }

    @Override
    public Status jobStatus() {
      return Status.SUCCEEDED;
    }
  }

  @Test
  public void testLegacyImplementorStillCompilesAndBehavesCorrectly() throws NoSuchJobException {
    JobHandle handle = new FakeJobHandle();
    SupportsJobs legacy = new LegacyJobsImpl(handle);

    // The plain lookup, and the two-arg overload with includeOutput=false, both delegate to the
    // legacy implementor's only method and behave identically.
    Assertions.assertSame(handle, legacy.getJob("job-1"));
    Assertions.assertSame(handle, legacy.getJob("job-1", false));

    // Requesting output from an implementor that never opted in throws a clear signal rather
    // than silently ignoring the request and returning a handle with no output.
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> legacy.getJob("job-1", true));
  }

  @Test
  public void testDefaultJobHandleStdoutAndStderrAreEmpty() {
    JobHandle handle = new FakeJobHandle();
    Assertions.assertEquals(Collections.emptyList(), handle.stdout());
    Assertions.assertEquals(Collections.emptyList(), handle.stderr());
  }
}
