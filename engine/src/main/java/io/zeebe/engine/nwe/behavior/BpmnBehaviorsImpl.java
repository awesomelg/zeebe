/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe.behavior;

import io.zeebe.engine.processor.TypedCommandWriter;
import io.zeebe.engine.processor.TypedStreamWriter;
import io.zeebe.engine.processor.workflow.CatchEventBehavior;
import io.zeebe.engine.processor.workflow.ExpressionProcessor;
import io.zeebe.engine.processor.workflow.handlers.IOMappingHelper;

public final class BpmnBehaviorsImpl implements BpmnBehaviors {

  private final ExpressionProcessor expressionBehavior;
  private final IOMappingHelper variableMappingBehavior;
  private final CatchEventBehavior eventSubscriptionBehavior;
  private final BpmnIncidentBehavior incidentBehavior;
  private final BpmnStateBehavior stateBehavior;
  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final TypedStreamWriter streamWriter;
  private final DeferredRecordsBehavior deferredRecordsBehavior;

  public BpmnBehaviorsImpl(
      final ExpressionProcessor expressionBehavior,
      final IOMappingHelper variableMappingBehavior,
      final CatchEventBehavior eventSubscriptionBehavior,
      final BpmnIncidentBehavior incidentBehavior,
      final BpmnStateBehavior stateBehavior,
      final BpmnStateTransitionBehavior stateTransitionBehavior,
      final TypedStreamWriter streamWriter,
      final DeferredRecordsBehavior deferredRecordsBehavior) {
    this.expressionBehavior = expressionBehavior;
    this.variableMappingBehavior = variableMappingBehavior;
    this.eventSubscriptionBehavior = eventSubscriptionBehavior;
    this.incidentBehavior = incidentBehavior;
    this.stateBehavior = stateBehavior;
    this.stateTransitionBehavior = stateTransitionBehavior;
    this.streamWriter = streamWriter;
    this.deferredRecordsBehavior = deferredRecordsBehavior;
  }

  @Override
  public ExpressionProcessor expressionBehavior() {
    return expressionBehavior;
  }

  @Override
  public IOMappingHelper variableMappingBehavior() {
    return variableMappingBehavior;
  }

  @Override
  public CatchEventBehavior eventSubscriptionBehavior() {
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
  public DeferredRecordsBehavior deferredRecordsBehavior() {
    return deferredRecordsBehavior;
  }
}
