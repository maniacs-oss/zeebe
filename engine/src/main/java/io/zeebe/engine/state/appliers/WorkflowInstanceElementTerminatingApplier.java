package io.zeebe.engine.state.appliers;

import io.zeebe.engine.state.TypedEventApplier;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.mutable.MutableElementInstanceState;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;

/** Applies state changes for `WorkflowInstance:Element_Terminating` */
final class WorkflowInstanceElementTerminatingApplier
    implements TypedEventApplier<WorkflowInstanceIntent, WorkflowInstanceRecord> {

  private final MutableElementInstanceState elementInstanceState;

  public WorkflowInstanceElementTerminatingApplier(final ZeebeState state) {
    elementInstanceState = state.getElementInstanceState();
  }

  @Override
  public void applyState(final long key, final WorkflowInstanceRecord value) {
    elementInstanceState.updateInstance(
        key, instance -> instance.setState(WorkflowInstanceIntent.ELEMENT_TERMINATING));
  }
}
