# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import unittest
from datetime import datetime, timezone

from gravitino.api.job.job_handle import JobHandle
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.job.job_dto import JobDTO


class TestJobDTOSerDe(unittest.TestCase):

    def test_ser_de_with_finished_at(self):
        queued_at = datetime.now(timezone.utc)
        started_at = datetime.now(timezone.utc)
        finished_at = datetime.now(timezone.utc)
        job_dto = JobDTO(
            _job_id="job-123",
            _job_template_name="test_template",
            _status=JobHandle.Status.SUCCEEDED,
            _audit=AuditDTO(_creator="test", _create_time=datetime.now(timezone.utc)),
            _queued_at=queued_at,
            _started_at=started_at,
            _finished_at=finished_at,
        )

        json_str = job_dto.to_json()
        self.assertIn("queuedAt", json_str)
        self.assertIn("startedAt", json_str)
        self.assertIn("finishedAt", json_str)

        deser_job_dto = JobDTO.from_json(json_str)
        self.assertEqual(job_dto, deser_job_dto)
        self.assertEqual(queued_at, deser_job_dto.queued_at())
        self.assertEqual(started_at, deser_job_dto.started_at())
        self.assertEqual(finished_at, deser_job_dto.finished_at())

    def test_ser_de_with_none_started_and_finished_at(self):
        queued_at = datetime.now(timezone.utc)
        job_dto = JobDTO(
            _job_id="job-456",
            _job_template_name="test_template",
            _status=JobHandle.Status.QUEUED,
            _audit=AuditDTO(_creator="test", _create_time=datetime.now(timezone.utc)),
            _queued_at=queued_at,
        )

        json_str = job_dto.to_json()
        deser_job_dto = JobDTO.from_json(json_str)
        self.assertEqual(job_dto, deser_job_dto)
        self.assertEqual(queued_at, deser_job_dto.queued_at())
        self.assertIsNone(deser_job_dto.started_at())
        self.assertIsNone(deser_job_dto.finished_at())

    def test_deserialize_from_string(self):
        json_str = (
            '{"jobId": "job-789", "jobTemplateName": "test_template", '
            '"status": "failed", '
            '"audit": {"creator": "test", "createTime": "2024-01-01T00:00:00Z"}, '
            '"queuedAt": "2024-01-01T00:00:00Z", '
            '"startedAt": "2024-01-01T00:30:00Z", '
            '"finishedAt": "2024-01-01T01:00:00Z"}'
        )

        job_dto = JobDTO.from_json(json_str)

        self.assertEqual("job-789", job_dto.job_id())
        self.assertEqual("test_template", job_dto.job_template_name())
        self.assertEqual(JobHandle.Status.FAILED, job_dto.status())
        self.assertEqual(
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc), job_dto.queued_at()
        )
        self.assertEqual(
            datetime(2024, 1, 1, 0, 30, 0, tzinfo=timezone.utc), job_dto.started_at()
        )
        self.assertEqual(
            datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc), job_dto.finished_at()
        )

    def test_deserialize_from_string_without_started_or_finished_at(self):
        json_str = (
            '{"jobId": "job-1000", "jobTemplateName": "test_template", '
            '"status": "queued", '
            '"audit": {"creator": "test", "createTime": "2024-01-01T00:00:00Z"}, '
            '"queuedAt": "2024-01-01T00:00:00Z"}'
        )

        job_dto = JobDTO.from_json(json_str, infer_missing=True)

        self.assertEqual("job-1000", job_dto.job_id())
        self.assertEqual(
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc), job_dto.queued_at()
        )
        self.assertIsNone(job_dto.started_at())
        self.assertIsNone(job_dto.finished_at())
