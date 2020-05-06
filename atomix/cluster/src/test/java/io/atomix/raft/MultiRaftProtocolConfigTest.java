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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.Partitioner;
import org.junit.Test;

/** Multi-Raft protocol configuration. */
public class MultiRaftProtocolConfigTest {

  @Test
  public void testConfig() throws Exception {
    final MultiRaftProtocolConfig config = new MultiRaftProtocolConfig();
    assertEquals(MultiRaftProtocol.TYPE, config.getType());
    assertNull(config.getGroup());
    assertSame(Partitioner.MURMUR3, config.getPartitioner());
    assertEquals(Recovery.RECOVER, config.getRecoveryStrategy());
    assertEquals(0, config.getMaxRetries());

    final Partitioner<String> partitioner = (k, p) -> null;
    config.setGroup("test");
    config.setPartitioner(partitioner);
    config.setRecoveryStrategy(Recovery.CLOSE);
    config.setMaxRetries(5);

    assertEquals("test", config.getGroup());
    assertSame(partitioner, config.getPartitioner());
    assertEquals(Recovery.CLOSE, config.getRecoveryStrategy());
    assertEquals(5, config.getMaxRetries());
  }
}
