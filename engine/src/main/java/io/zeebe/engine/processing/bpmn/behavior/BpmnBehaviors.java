/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.bpmn.behavior;

import io.zeebe.engine.processing.bpmn.WorkflowInstanceStateTransitionGuard;
import io.zeebe.engine.processing.common.ExpressionProcessor;
import io.zeebe.engine.processing.streamprocessor.writers.TypedCommandWriter;

public interface BpmnBehaviors {

  ExpressionProcessor expressionBehavior();

  BpmnVariableMappingBehavior variableMappingBehavior();

  BpmnEventPublicationBehavior eventPublicationBehavior();

  BpmnEventSubscriptionBehavior eventSubscriptionBehavior();

  BpmnIncidentBehavior incidentBehavior();

  BpmnStateBehavior stateBehavior();

  TypedCommandWriter commandWriter();

  BpmnStateTransitionBehavior stateTransitionBehavior();

  BpmnDeferredRecordsBehavior deferredRecordsBehavior();

  WorkflowInstanceStateTransitionGuard stateTransitionGuard();

  BpmnWorkflowResultSenderBehavior workflowResultSenderBehavior();

  BpmnBufferedMessageStartEventBehavior bufferedMessageStartEventBehavior();
}
