/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe.task;

import io.zeebe.engine.Loggers;
import io.zeebe.engine.nwe.BpmnElementContext;
import io.zeebe.engine.nwe.BpmnElementProcessor;
import io.zeebe.engine.nwe.behavior.BpmnBehaviors;
import io.zeebe.engine.nwe.behavior.BpmnEventSubscriptionBehavior;
import io.zeebe.engine.nwe.behavior.BpmnIncidentBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateTransitionBehavior;
import io.zeebe.engine.processor.Failure;
import io.zeebe.engine.processor.TypedCommandWriter;
import io.zeebe.engine.processor.workflow.ExpressionProcessor;
import io.zeebe.engine.processor.workflow.ExpressionProcessor.EvaluationException;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableServiceTask;
import io.zeebe.engine.processor.workflow.handlers.IOMappingHelper;
import io.zeebe.engine.processor.workflow.message.MessageCorrelationKeyException;
import io.zeebe.engine.state.instance.JobState.State;
import io.zeebe.msgpack.value.DocumentValue;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.value.ErrorType;
import io.zeebe.util.Either;

public final class ServiceTaskProcessor implements BpmnElementProcessor<ExecutableServiceTask> {

  private final JobRecord jobCommand = new JobRecord().setVariables(DocumentValue.EMPTY_DOCUMENT);

  private final IOMappingHelper variableMappingBehavior;
  private final ExpressionProcessor expressionBehavior;
  private final TypedCommandWriter commandWriter;
  private final BpmnIncidentBehavior incidentBehavior;
  private final BpmnStateBehavior stateBehavior;
  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final BpmnEventSubscriptionBehavior eventSubscriptionBehavior;

  public ServiceTaskProcessor(final BpmnBehaviors behaviors) {
    variableMappingBehavior = behaviors.variableMappingBehavior();
    eventSubscriptionBehavior = behaviors.eventSubscriptionBehavior();
    expressionBehavior = behaviors.expressionBehavior();
    commandWriter = behaviors.commandWriter();
    incidentBehavior = behaviors.incidentBehavior();
    stateBehavior = behaviors.stateBehavior();
    stateTransitionBehavior = behaviors.stateTransitionBehavior();
  }

  @Override
  public Class<ExecutableServiceTask> getType() {
    return ExecutableServiceTask.class;
  }

  @Override
  public void onActivating(final ExecutableServiceTask element, final BpmnElementContext context) {

    // TODO (saig0): migrate to Either types
    final var success = variableMappingBehavior.applyInputMappings(context.toStepContext());
    if (!success) {
      return;
    }

    try {
      eventSubscriptionBehavior.subscribeToEvents(element, context);
    } catch (final MessageCorrelationKeyException e) {
      incidentBehavior.createIncident(
          ErrorType.EXTRACT_VALUE_ERROR,
          e.getMessage(),
          context,
          e.getContext().getVariablesScopeKey());
      return;
    } catch (final EvaluationException e) {
      incidentBehavior.createIncident(
          ErrorType.EXTRACT_VALUE_ERROR, e.getMessage(), context, context.getElementInstanceKey());
      return;
    }

    stateTransitionBehavior.transitionToActivated(context);
  }

  @Override
  public void onActivated(final ExecutableServiceTask element, final BpmnElementContext context) {
    final var scopeKey = context.getElementInstanceKey();
    final Either<Failure, String> jobTypeOrFailure =
        expressionBehavior.evaluateStringExpression(element.getType(), scopeKey);
    jobTypeOrFailure
        .flatMap(
            jobType -> expressionBehavior.evaluateLongExpression(element.getRetries(), scopeKey))
        .ifRightOrLeft(
            retries -> createNewJob(context, element, jobTypeOrFailure.get(), retries.intValue()),
            failure -> incidentBehavior.createIncident(failure, context, scopeKey));
  }

  @Override
  public void onCompleting(final ExecutableServiceTask element, final BpmnElementContext context) {

    // TODO (saig0): extract guard check and perform also on other transitions
    final var flowScopeInstance = stateBehavior.getFlowScopeInstance(context);
    if (!flowScopeInstance.isActive()) {
      return;
    }

    final var success = variableMappingBehavior.applyOutputMappings(context.toStepContext());
    if (!success) {
      return;
    }

    eventSubscriptionBehavior.unsubscribeFromEvents(context);

    stateTransitionBehavior.transitionToCompleted(context);
  }

  @Override
  public void onCompleted(final ExecutableServiceTask element, final BpmnElementContext context) {

    stateTransitionBehavior.takeOutgoingSequenceFlows(element, context);

    stateBehavior.consumeToken(context);
    stateBehavior.removeInstance(context);
  }

  @Override
  public void onTerminating(final ExecutableServiceTask element, final BpmnElementContext context) {

    final var elementInstance = stateBehavior.getElementInstance(context);
    final long jobKey = elementInstance.getJobKey();
    if (jobKey > 0) {
      cancelJob(jobKey);
      incidentBehavior.resolveJobIncident(jobKey);
    }

    eventSubscriptionBehavior.unsubscribeFromEvents(context);

    stateTransitionBehavior.transitionToTerminated(context);
  }

  @Override
  public void onTerminated(final ExecutableServiceTask element, final BpmnElementContext context) {

    eventSubscriptionBehavior.publishTriggeredBoundaryEvent(context);

    incidentBehavior.resolveIncidents(context);

    stateTransitionBehavior.onElementTerminated(element, context);

    stateBehavior.consumeToken(context);
  }

  @Override
  public void onEventOccurred(
      final ExecutableServiceTask element, final BpmnElementContext context) {

    eventSubscriptionBehavior.triggerBoundaryEvent(element, context);
  }

  private void createNewJob(
      final BpmnElementContext context,
      final ExecutableServiceTask serviceTask,
      final String jobType,
      final int retries) {

    jobCommand
        .setType(jobType)
        .setRetries(retries)
        .setCustomHeaders(serviceTask.getEncodedHeaders())
        .setBpmnProcessId(context.getBpmnProcessId())
        .setWorkflowDefinitionVersion(context.getWorkflowVersion())
        .setWorkflowKey(context.getWorkflowKey())
        .setWorkflowInstanceKey(context.getWorkflowInstanceKey())
        .setElementId(serviceTask.getId())
        .setElementInstanceKey(context.getElementInstanceKey());

    commandWriter.appendNewCommand(JobIntent.CREATE, jobCommand);
  }

  private void cancelJob(final long jobKey) {
    final State state = stateBehavior.getJobState().getState(jobKey);

    if (state == State.NOT_FOUND) {
      Loggers.WORKFLOW_PROCESSOR_LOGGER.warn(
          "Expected to find job with key {}, but no job found", jobKey);

    } else if (state == State.ACTIVATABLE || state == State.ACTIVATED || state == State.FAILED) {
      final JobRecord job = stateBehavior.getJobState().getJob(jobKey);
      commandWriter.appendFollowUpCommand(jobKey, JobIntent.CANCEL, job);
    }
  }
}
