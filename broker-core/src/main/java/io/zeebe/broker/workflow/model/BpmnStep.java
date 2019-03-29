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
package io.zeebe.broker.workflow.model;

public enum BpmnStep {

  // new steps are just lifecycle state
  ELEMENT_ACTIVATING,
  ELEMENT_ACTIVATED,
  EVENT_OCCURRED,
  ELEMENT_COMPLETING,
  ELEMENT_COMPLETED,
  ELEMENT_TERMINATING,
  ELEMENT_TERMINATED,

  ACTIVITY_ELEMENT_ACTIVATING,
  ACTIVITY_ELEMENT_ACTIVATED,
  ACTIVITY_EVENT_OCCURRED,
  ACTIVITY_ELEMENT_COMPLETING,
  ACTIVITY_ELEMENT_TERMINATING,
  ACTIVITY_ELEMENT_TERMINATED,

  CONTAINER_ELEMENT_ACTIVATED,
  CONTAINER_ELEMENT_TERMINATING,

  EVENT_BASED_GATEWAY_ELEMENT_ACTIVATING,
  EVENT_BASED_GATEWAY_ELEMENT_ACTIVATED,
  EVENT_BASED_GATEWAY_EVENT_OCCURRED,
  EVENT_BASED_GATEWAY_ELEMENT_COMPLETING,
  EVENT_BASED_GATEWAY_ELEMENT_COMPLETED,
  EVENT_BASED_GATEWAY_ELEMENT_TERMINATING,

  EXCLUSIVE_GATEWAY_ELEMENT_ACTIVATING,
  EXCLUSIVE_GATEWAY_ELEMENT_COMPLETED,

  FLOWOUT_ELEMENT_COMPLETED,

  INTERMEDIATE_CATCH_EVENT_ELEMENT_ACTIVATING,
  INTERMEDIATE_CATCH_EVENT_ELEMENT_ACTIVATED,
  INTERMEDIATE_CATCH_EVENT_EVENT_OCCURRED,
  INTERMEDIATE_CATCH_EVENT_ELEMENT_COMPLETING,
  INTERMEDIATE_CATCH_EVENT_ELEMENT_TERMINATING,

  PARALLEL_MERGE_SEQUENCE_FLOW_TAKEN,

  RECEIVE_TASK_EVENT_OCCURRED,

  SEQUENCE_FLOW_TAKEN,

  SERVICE_TASK_ELEMENT_ACTIVATED,
  SERVICE_TASK_ELEMENT_TERMINATING,

  START_EVENT_EVENT_OCCURRED,
}
