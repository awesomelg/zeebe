/*
 * Zeebe Broker Core
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
package io.zeebe.broker.workflow.processor.instance;

import static io.zeebe.util.buffer.BufferUtil.bufferAsString;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.logstreams.processor.CommandProcessor;
import io.zeebe.broker.logstreams.processor.KeyGenerator;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.workflow.state.DeployedWorkflow;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.broker.workflow.state.ElementInstanceState;
import io.zeebe.broker.workflow.state.VariablesState;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.msgpack.spec.MsgpackReaderException;
import io.zeebe.protocol.BpmnElementType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceCreationRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceCreationIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import org.agrona.DirectBuffer;

public class CreateWorkflowInstanceProcessor
    implements CommandProcessor<WorkflowInstanceCreationRecord> {

  private static final String ERROR_MESSAGE_NO_IDENTIFIER_SPECIFIED =
      "Expected at least a bpmnProcessId or a key greater than -1, but none given";
  private static final String ERROR_MESSAGE_NOT_FOUND_BY_PROCESS =
      "Expected to find workflow definition with process ID '%s', but none found";
  private static final String ERROR_MESSAGE_NOT_FOUND_BY_PROCESS_AND_VERSION =
      "Expected to find workflow definition with process ID '%s' and version '%d', but none found";
  private static final String ERROR_MESSAGE_NOT_FOUND_BY_KEY =
      "Expected to find workflow definition with key '%d', but none found";
  private static final String ERROR_MESSAGE_NO_NONE_START_EVENT =
      "Expected to create instance of workflow with none start event, but there is no such event";
  private static final String ERROR_INVALID_VARIABLES_REJECTION_MESSAGE =
      "Expected to set variables from document, but the document is invalid: '%s'";
  private static final String ERROR_INVALID_VARIABLES_LOGGED_MESSAGE =
      "Expected to set variables from document, but the document is invalid";

  private final WorkflowState workflowState;
  private final ElementInstanceState elementInstanceState;
  private final VariablesState variablesState;
  private final KeyGenerator keyGenerator;

  private final WorkflowInstanceRecord newWorkflowInstance = new WorkflowInstanceRecord();

  public CreateWorkflowInstanceProcessor(
      WorkflowState workflowState,
      ElementInstanceState elementInstanceState,
      VariablesState variablesState,
      KeyGenerator keyGenerator) {
    this.workflowState = workflowState;
    this.elementInstanceState = elementInstanceState;
    this.variablesState = variablesState;
    this.keyGenerator = keyGenerator;
  }

  @Override
  public void onCommand(
      TypedRecord<WorkflowInstanceCreationRecord> command,
      CommandControl<WorkflowInstanceCreationRecord> controller,
      TypedStreamWriter streamWriter) {
    final WorkflowInstanceCreationRecord record = command.getValue();
    final DeployedWorkflow workflow = getWorkflow(record, controller);
    if (workflow == null || !isValidWorkflow(controller, workflow)) {
      return;
    }

    final long workflowInstanceKey = keyGenerator.nextKey();
    if (!setVariablesFromDocument(controller, record, workflowInstanceKey)) {
      return;
    }

    final ElementInstance workflowInstance =
        createElementInstance(workflow, workflowInstanceKey, record.getVariables());
    streamWriter.appendFollowUpEvent(
        workflowInstanceKey,
        WorkflowInstanceIntent.ELEMENT_ACTIVATING,
        workflowInstance.getValue());
    controller.accept(
        WorkflowInstanceCreationIntent.CREATED, record.setInstanceKey(workflowInstanceKey));
  }

  private boolean isValidWorkflow(
      CommandControl<WorkflowInstanceCreationRecord> controller, DeployedWorkflow workflow) {
    if (workflow.getWorkflow().getNoneStartEvent() == null) {
      controller.reject(RejectionType.INVALID_STATE, ERROR_MESSAGE_NO_NONE_START_EVENT);
      return false;
    }

    return true;
  }

  private boolean setVariablesFromDocument(
      CommandControl<WorkflowInstanceCreationRecord> controller,
      WorkflowInstanceCreationRecord record,
      long workflowInstanceKey) {
    try {
      variablesState.setVariablesLocalFromDocument(workflowInstanceKey, record.getVariables());
    } catch (MsgpackReaderException e) {
      Loggers.WORKFLOW_PROCESSOR_LOGGER.error(ERROR_INVALID_VARIABLES_LOGGED_MESSAGE, e);
      controller.reject(
          RejectionType.INVALID_ARGUMENT,
          String.format(ERROR_INVALID_VARIABLES_REJECTION_MESSAGE, e.getMessage()));

      return false;
    }

    return true;
  }

  private ElementInstance createElementInstance(
      DeployedWorkflow workflow, long workflowInstanceKey, DirectBuffer variables) {
    newWorkflowInstance.reset();
    newWorkflowInstance.setBpmnProcessId(workflow.getBpmnProcessId());
    newWorkflowInstance.setVersion(workflow.getVersion());
    newWorkflowInstance.setWorkflowKey(workflow.getKey());
    newWorkflowInstance.setWorkflowInstanceKey(workflowInstanceKey);
    newWorkflowInstance.setBpmnElementType(BpmnElementType.PROCESS);
    newWorkflowInstance.setElementId(workflow.getWorkflow().getId());
    newWorkflowInstance.setFlowScopeKey(-1);
    newWorkflowInstance.setPayload(variables); // todo: to be removed in follow up issues (#1858)

    final ElementInstance instance =
        elementInstanceState.newInstance(
            workflowInstanceKey, newWorkflowInstance, WorkflowInstanceIntent.ELEMENT_ACTIVATING);
    elementInstanceState.flushDirtyState();

    return instance;
  }

  private DeployedWorkflow getWorkflow(
      WorkflowInstanceCreationRecord record, CommandControl controller) {
    final DeployedWorkflow workflow;

    if (record.getKey() >= 0) {
      workflow = getWorkflow(record.getKey(), controller);
    } else {
      final DirectBuffer bpmnProcessId = record.getBpmnProcessId();

      if (bpmnProcessId.capacity() == 0) {
        controller.reject(RejectionType.INVALID_ARGUMENT, ERROR_MESSAGE_NO_IDENTIFIER_SPECIFIED);
        workflow = null;
      } else if (record.getVersion() >= 0) {
        workflow = getWorkflow(bpmnProcessId, record.getVersion(), controller);
      } else {
        workflow = getWorkflow(bpmnProcessId, controller);
      }
    }

    return workflow;
  }

  private DeployedWorkflow getWorkflow(DirectBuffer bpmnProcessId, CommandControl controller) {
    final DeployedWorkflow workflow =
        workflowState.getLatestWorkflowVersionByProcessId(bpmnProcessId);
    if (workflow == null) {
      controller.reject(
          RejectionType.NOT_FOUND,
          String.format(ERROR_MESSAGE_NOT_FOUND_BY_PROCESS, bufferAsString(bpmnProcessId)));
    }

    return workflow;
  }

  private DeployedWorkflow getWorkflow(
      DirectBuffer bpmnProcessId, int version, CommandControl controller) {
    final DeployedWorkflow workflow =
        workflowState.getWorkflowByProcessIdAndVersion(bpmnProcessId, version);
    if (workflow == null) {
      controller.reject(
          RejectionType.NOT_FOUND,
          String.format(
              ERROR_MESSAGE_NOT_FOUND_BY_PROCESS_AND_VERSION,
              bufferAsString(bpmnProcessId),
              version));
    }

    return workflow;
  }

  private DeployedWorkflow getWorkflow(long key, CommandControl controller) {
    final DeployedWorkflow workflow = workflowState.getWorkflowByKey(key);
    if (workflow == null) {
      controller.reject(
          RejectionType.NOT_FOUND, String.format(ERROR_MESSAGE_NOT_FOUND_BY_KEY, key));
    }

    return workflow;
  }
}