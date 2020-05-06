/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe.behavior;

import io.zeebe.engine.nwe.BpmnElementContext;
import io.zeebe.engine.processor.KeyGenerator;
import io.zeebe.engine.processor.TypedStreamWriter;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.instance.ElementInstance;
import io.zeebe.engine.state.instance.ElementInstanceState;
import io.zeebe.engine.state.instance.IndexedRecord;
import io.zeebe.engine.state.instance.StoredRecord.Purpose;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import java.util.List;

public class BpmnDeferredRecordsBehavior {

  private final TypedStreamWriter streamWriter;
  private final ElementInstanceState elementInstanceState;
  private final KeyGenerator keyGenerator;

  private final BpmnStateBehavior stateBehavior;

  public BpmnDeferredRecordsBehavior(
      final ZeebeState zeebeState,
      final TypedStreamWriter streamWriter,
      final BpmnStateBehavior stateBehavior) {
    elementInstanceState = zeebeState.getWorkflowState().getElementInstanceState();
    this.streamWriter = streamWriter;
    keyGenerator = zeebeState.getKeyGenerator();
    this.stateBehavior = stateBehavior;
  }

  public long deferNewRecord(
      final long scopeKey, final WorkflowInstanceRecord value, final WorkflowInstanceIntent state) {
    final long key = keyGenerator.nextKey();
    elementInstanceState.storeRecord(key, scopeKey, value, state, Purpose.DEFERRED);
    return key;
  }

  public List<IndexedRecord> getDeferredRecords(final BpmnElementContext context) {
    return elementInstanceState.getDeferredRecords(context.getElementInstanceKey());
  }

  public void publishDeferredRecords(final BpmnElementContext context) {
    final List<IndexedRecord> deferredRecords =
        elementInstanceState.getDeferredRecords(context.getElementInstanceKey());

    final ElementInstance flowScopeInstance = stateBehavior.getFlowScopeInstance(context);

    for (final IndexedRecord record : deferredRecords) {
      publishDeferredRecord(context, flowScopeInstance, record);
      stateBehavior.spawnToken(context);
    }
  }

  private void publishDeferredRecord(
      final BpmnElementContext context,
      final ElementInstance flowScopeInstance,
      final IndexedRecord record) {

    if (record.getState() != WorkflowInstanceIntent.ELEMENT_ACTIVATING) {
      throw new IllegalStateException(
          String.format(
              "Expected to publish a deferred record with intent '%s' but its intent was '%s'. [context: %s]",
              WorkflowInstanceIntent.ELEMENT_ACTIVATING, record.getState(), context));
    }

    record.getValue().setFlowScopeKey(flowScopeInstance.getKey());

    elementInstanceState.newInstance(
        flowScopeInstance,
        record.getKey(),
        record.getValue(),
        WorkflowInstanceIntent.ELEMENT_ACTIVATING);

    streamWriter.appendFollowUpEvent(
        record.getKey(), WorkflowInstanceIntent.ELEMENT_ACTIVATING, record.getValue());
  }

  // TODO (saig0): extract to event related behavior?
  public void publishInterruptingEvent(final BpmnElementContext context) {
    final var flowScopeInstance = stateBehavior.getFlowScopeInstance(context);

    elementInstanceState.getDeferredRecords(context.getFlowScopeKey()).stream()
        .filter(r -> r.getKey() == flowScopeInstance.getInterruptingEventKey())
        .findFirst()
        .ifPresent(record -> publishDeferredRecord(context, flowScopeInstance, record));
  }
}
