/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe.behavior;

import io.zeebe.engine.metrics.WorkflowEngineMetrics;
import io.zeebe.engine.nwe.BpmnElementContainerProcessor;
import io.zeebe.engine.processor.TypedCommandWriter;
import io.zeebe.engine.processor.TypedStreamWriter;
import io.zeebe.engine.processor.workflow.CatchEventBehavior;
import io.zeebe.engine.processor.workflow.ExpressionProcessor;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableFlowElement;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.protocol.record.value.BpmnElementType;
import java.util.function.Function;

public final class BpmnBehaviorsImpl implements BpmnBehaviors {

  private final ExpressionProcessor expressionBehavior;
  private final BpmnVariableMappingBehavior variableMappingBehavior;
  private final BpmnEventSubscriptionBehavior eventSubscriptionBehavior;
  private final BpmnIncidentBehavior incidentBehavior;
  private final BpmnStateBehavior stateBehavior;
  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final TypedStreamWriter streamWriter;
  private final BpmnDeferredRecordsBehavior deferredRecordsBehavior;

  public BpmnBehaviorsImpl(
      final ExpressionProcessor expressionBehavior,
      final TypedStreamWriter streamWriter,
      final ZeebeState zeebeState,
      final CatchEventBehavior catchEventBehavior,
      final Function<BpmnElementType, BpmnElementContainerProcessor<ExecutableFlowElement>>
          processorLookup) {
    this.stateBehavior = new BpmnStateBehavior(zeebeState);
    this.expressionBehavior = expressionBehavior;
    this.variableMappingBehavior = new BpmnVariableMappingBehavior(expressionBehavior, zeebeState);
    this.stateTransitionBehavior =
        new BpmnStateTransitionBehavior(
            streamWriter,
            zeebeState.getKeyGenerator(),
            stateBehavior,
            new WorkflowEngineMetrics(zeebeState.getPartitionId()),
            processorLookup);
    this.eventSubscriptionBehavior =
        new BpmnEventSubscriptionBehavior(
            stateBehavior, stateTransitionBehavior, catchEventBehavior, streamWriter, zeebeState);
    this.incidentBehavior = new BpmnIncidentBehavior(zeebeState, streamWriter);
    this.streamWriter = streamWriter;
    this.deferredRecordsBehavior = new BpmnDeferredRecordsBehavior(zeebeState);
  }

  @Override
  public ExpressionProcessor expressionBehavior() {
    return expressionBehavior;
  }

  @Override
  public BpmnVariableMappingBehavior variableMappingBehavior() {
    return variableMappingBehavior;
  }

  @Override
  public BpmnEventSubscriptionBehavior eventSubscriptionBehavior() {
    return eventSubscriptionBehavior;
  }

  @Override
  public BpmnIncidentBehavior incidentBehavior() {
    return incidentBehavior;
  }

  @Override
  public BpmnStateBehavior stateBehavior() {
    return stateBehavior;
  }

  @Override
  public TypedCommandWriter commandWriter() {
    return streamWriter;
  }

  @Override
  public BpmnStateTransitionBehavior stateTransitionBehavior() {
    return stateTransitionBehavior;
  }

  @Override
  public BpmnDeferredRecordsBehavior deferredRecordsBehavior() {
    return deferredRecordsBehavior;
  }
}
