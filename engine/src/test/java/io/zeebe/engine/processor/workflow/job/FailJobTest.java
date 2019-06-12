/*
 * Zeebe Workflow Engine
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.engine.processor.workflow.job;

import static io.zeebe.protocol.intent.JobIntent.ACTIVATED;
import static io.zeebe.protocol.intent.JobIntent.FAIL;
import static io.zeebe.protocol.intent.JobIntent.FAILED;
import static io.zeebe.test.util.record.RecordingExporter.jobRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.exporter.api.record.Assertions;
import io.zeebe.exporter.api.record.Record;
import io.zeebe.exporter.api.record.RecordMetadata;
import io.zeebe.exporter.api.record.value.JobBatchRecordValue;
import io.zeebe.exporter.api.record.value.JobRecordValue;
import io.zeebe.protocol.RecordType;
import io.zeebe.protocol.RejectionType;
import io.zeebe.protocol.ValueType;
import io.zeebe.protocol.intent.JobBatchIntent;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.test.util.MsgPackUtil;
import io.zeebe.test.util.Strings;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class FailJobTest {
  private static final String JSON_VARIABLES = "{\"foo\":\"bar\"}";
  private static final byte[] VARIABLES_MSG_PACK = MsgPackUtil.asMsgPackReturnArray(JSON_VARIABLES);
  private static final String PROCESS_ID = "process";
  private static String jobType;

  @ClassRule public static EngineRule engineRule = new EngineRule();

  @Before
  public void setup() {
    jobType = Strings.newRandomValidBpmnId();
  }

  @Test
  public void shouldFail() {
    // given
    engineRule.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();
    final JobRecordValue job = batchRecord.getValue().getJobs().get(0);
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);
    final int retries = 23;

    // when
    final Record<JobRecordValue> failRecord =
        engineRule
            .job()
            .ofInstance(job.getHeaders().getWorkflowInstanceKey())
            .withRetries(retries)
            .fail(jobKey);

    // then
    Assertions.assertThat(failRecord.getMetadata())
        .hasRecordType(RecordType.EVENT)
        .hasIntent(FAILED);
    Assertions.assertThat(failRecord.getValue())
        .hasWorker(job.getWorker())
        .hasType(job.getType())
        .hasRetries(retries)
        .hasDeadline(job.getDeadline());
  }

  @Test
  public void shouldFailWithMessage() {
    // given
    engineRule.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();

    final long jobKey = batchRecord.getValue().getJobKeys().get(0);
    final JobRecordValue job = batchRecord.getValue().getJobs().get(0);
    final int retries = 23;

    // when
    final Record<JobRecordValue> failedRecord =
        engineRule
            .job()
            .ofInstance(job.getHeaders().getWorkflowInstanceKey())
            .withRetries(retries)
            .withErrorMessage("failed job")
            .fail(jobKey);

    // then
    Assertions.assertThat(failedRecord.getMetadata())
        .hasRecordType(RecordType.EVENT)
        .hasIntent(FAILED);
    Assertions.assertThat(failedRecord.getValue())
        .hasWorker(job.getWorker())
        .hasType(job.getType())
        .hasRetries(retries)
        .hasDeadline(job.getDeadline())
        .hasErrorMessage(failedRecord.getValue().getErrorMessage());
  }

  @Test
  public void shouldFailJobAndRetry() {
    // given
    final Record<JobRecordValue> job = engineRule.createJob(jobType, PROCESS_ID);

    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);
    final Record<JobRecordValue> activatedRecord =
        jobRecords(ACTIVATED).withRecordKey(jobKey).getFirst();

    // when
    final Record<JobRecordValue> failRecord =
        engineRule
            .job()
            .ofInstance(job.getValue().getHeaders().getWorkflowInstanceKey())
            .withRetries(3)
            .fail(jobKey);
    engineRule.jobs().withType(jobType).activate();

    // then
    Assertions.assertThat(failRecord.getMetadata())
        .hasRecordType(RecordType.EVENT)
        .hasIntent(FAILED);

    // and the job is published again
    final Record republishedEvent =
        RecordingExporter.jobRecords()
            .skipUntil(j -> j.getMetadata().getIntent() == FAILED)
            .withIntent(ACTIVATED)
            .getFirst();

    assertThat(republishedEvent.getKey()).isEqualTo(activatedRecord.getKey());
    assertThat(republishedEvent.getPosition()).isNotEqualTo(activatedRecord.getPosition());

    // and the job lifecycle is correct
    final List<Record> jobEvents =
        RecordingExporter.jobRecords().limit(6).collect(Collectors.toList());
    assertThat(jobEvents)
        .extracting(Record::getMetadata)
        .extracting(
            RecordMetadata::getRecordType, RecordMetadata::getValueType, RecordMetadata::getIntent)
        .containsExactly(
            tuple(RecordType.COMMAND, ValueType.JOB, JobIntent.CREATE),
            tuple(RecordType.EVENT, ValueType.JOB, JobIntent.CREATED),
            tuple(RecordType.EVENT, ValueType.JOB, JobIntent.ACTIVATED),
            tuple(RecordType.COMMAND, ValueType.JOB, FAIL),
            tuple(RecordType.EVENT, ValueType.JOB, FAILED),
            tuple(RecordType.EVENT, ValueType.JOB, JobIntent.ACTIVATED));

    final List<Record<JobBatchRecordValue>> jobActivateCommands =
        RecordingExporter.jobBatchRecords().limit(4).collect(Collectors.toList());

    assertThat(jobActivateCommands)
        .extracting(Record::getMetadata)
        .extracting(
            RecordMetadata::getRecordType, RecordMetadata::getValueType, RecordMetadata::getIntent)
        .containsExactly(
            tuple(RecordType.COMMAND, ValueType.JOB_BATCH, JobBatchIntent.ACTIVATE),
            tuple(RecordType.EVENT, ValueType.JOB_BATCH, JobBatchIntent.ACTIVATED),
            tuple(RecordType.COMMAND, ValueType.JOB_BATCH, JobBatchIntent.ACTIVATE),
            tuple(RecordType.EVENT, ValueType.JOB_BATCH, JobBatchIntent.ACTIVATED));
  }

  @Test
  public void shouldRejectFailIfJobNotFound() {
    // given
    final int key = 123;

    // when
    final Record<JobRecordValue> jobRecord =
        engineRule.job().withRetries(3).expectRejection().fail(key);

    // then
    Assertions.assertThat(jobRecord.getMetadata()).hasRejectionType(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldRejectFailIfJobAlreadyFailed() {
    // given
    engineRule.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);
    engineRule.job().withRetries(0).fail(jobKey);

    // when
    final Record<JobRecordValue> jobRecord =
        engineRule.job().withRetries(3).expectRejection().fail(jobKey);

    // then
    Assertions.assertThat(jobRecord.getMetadata()).hasRejectionType(RejectionType.INVALID_STATE);
    assertThat(jobRecord.getMetadata().getRejectionReason()).contains("is marked as failed");
  }

  @Test
  public void shouldRejectFailIfJobCreated() {
    // given
    final Record<JobRecordValue> job = engineRule.createJob(jobType, PROCESS_ID);

    // when
    final Record<JobRecordValue> jobRecord =
        engineRule.job().withRetries(3).expectRejection().fail(job.getKey());

    // then
    Assertions.assertThat(jobRecord.getMetadata()).hasRejectionType(RejectionType.INVALID_STATE);
    assertThat(jobRecord.getMetadata().getRejectionReason()).contains("must be activated first");
  }

  @Test
  public void shouldRejectFailIfJobCompleted() {
    // given
    engineRule.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();
    final JobRecordValue job = batchRecord.getValue().getJobs().get(0);
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);

    engineRule.job().withVariables(MsgPackUtil.asMsgPack(job.getVariables())).complete(jobKey);

    // when
    final Record<JobRecordValue> jobRecord =
        engineRule.job().withRetries(3).expectRejection().fail(jobKey);

    // then
    Assertions.assertThat(jobRecord.getMetadata()).hasRejectionType(RejectionType.NOT_FOUND);
  }
}