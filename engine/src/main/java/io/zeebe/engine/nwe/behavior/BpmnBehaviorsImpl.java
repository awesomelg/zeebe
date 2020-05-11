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
import io.zeebe.engine.processor.workflow.ExpressionProcessor;

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
      final BpmnVariableMappingBehavior variableMappingBehavior,
      final BpmnEventSubscriptionBehavior eventSubscriptionBehavior,
      final BpmnIncidentBehavior incidentBehavior,
      final BpmnStateBehavior stateBehavior,
      final BpmnStateTransitionBehavior stateTransitionBehavior,
      final TypedStreamWriter streamWriter,
      final BpmnDeferredRecordsBehavior deferredRecordsBehavior) {
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
