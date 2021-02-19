package io.zeebe.engine.state.appliers;

import io.zeebe.engine.state.TypedEventApplier;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.mutable.MutableElementInstanceState;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;

/** Applies state changes for `WorkflowInstance:Sequence_Flow_Taken` */
final class WorkflowInstanceSequenceFlowTakenApplier
    implements TypedEventApplier<WorkflowInstanceIntent, WorkflowInstanceRecord> {

  private final MutableElementInstanceState elementInstanceState;

  public WorkflowInstanceSequenceFlowTakenApplier(final ZeebeState state) {
    elementInstanceState = state.getElementInstanceState();
  }

  @Override
  public void applyState(final long key, final WorkflowInstanceRecord value) {
    elementInstanceState.spawnToken(value.getFlowScopeKey());
  }
}
