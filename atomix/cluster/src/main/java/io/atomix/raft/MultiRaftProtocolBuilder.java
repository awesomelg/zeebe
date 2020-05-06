/*
 * Copyright 2018-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft;

import io.atomix.primitive.Recovery;
import io.atomix.primitive.protocol.PrimitiveProtocolBuilder;

/** Multi-Raft protocol builder. */
public class MultiRaftProtocolBuilder
    extends PrimitiveProtocolBuilder<
        MultiRaftProtocolBuilder, MultiRaftProtocolConfig, MultiRaftProtocol> {

  protected MultiRaftProtocolBuilder(final MultiRaftProtocolConfig config) {
    super(config);
  }

  /**
   * Sets the recovery strategy.
   *
   * @param recoveryStrategy the recovery strategy
   * @return the Raft protocol builder
   */
  public MultiRaftProtocolBuilder withRecoveryStrategy(final Recovery recoveryStrategy) {
    config.setRecoveryStrategy(recoveryStrategy);
    return this;
  }

  /**
   * Sets the maximum number of retries before an operation can be failed.
   *
   * @param maxRetries the maximum number of retries before an operation can be failed
   * @return the proxy builder
   */
  public MultiRaftProtocolBuilder withMaxRetries(final int maxRetries) {
    config.setMaxRetries(maxRetries);
    return this;
  }

  @Override
  public MultiRaftProtocol build() {
    return new MultiRaftProtocol(config);
  }
}
