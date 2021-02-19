package io.zeebe.engine.state.appliers;

import io.zeebe.engine.state.TypedEventApplier;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.mutable.MutableElementInstanceState;
import io.zeebe.engine.state.mutable.MutableEventScopeInstanceState;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;

/** Applies state changes for `WorkflowInstance:Element_Completed` */
final class WorkflowInstanceElementCompletedApplier
    implements TypedEventApplier<WorkflowInstanceIntent, WorkflowInstanceRecord> {

  private final MutableElementInstanceState elementInstanceState;
  private final MutableEventScopeInstanceState eventScopeInstanceState;

  public WorkflowInstanceElementCompletedApplier(final ZeebeState state) {
    elementInstanceState = state.getElementInstanceState();
    eventScopeInstanceState = state.getEventScopeInstanceState();
  }

  @Override
  public void applyState(final long key, final WorkflowInstanceRecord value) {
    eventScopeInstanceState.deleteInstance(key);
    elementInstanceState.consumeToken(value.getFlowScopeKey());
    elementInstanceState.removeInstance(key);
  }
}