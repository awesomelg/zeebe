/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.storage.journal.Indexed;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class RaftAppendTest {

  @Rule @Parameter public RaftRule raftRule;

  @Parameters(name = "{index}: {0}")
  public static Object[][] reprocessingTriggers() {
    return new Object[][] {
      new Object[] {new RaftRule(2)},
      new Object[] {new RaftRule(3)},
      new Object[] {new RaftRule(4)},
      new Object[] {new RaftRule(5)}
    };
  }

  @Test
  public void shouldAppendEntryOnAllNodes() throws Throwable {
    // given

    // when
    raftRule.appendEntry();

    // then
    raftRule.awaitCommit(2);
    raftRule.awaitSameLogSizeOnAllNodes();
    final var memberLog = raftRule.getMemberLog();

    final var logLength = memberLog.values().stream().map(List::size).findFirst().orElseThrow();
    assertThat(logLength).isEqualTo(1);

    assertMemberLogs(memberLog);
  }

  @Test
  public void shouldAppendEntriesOnAllNodes() throws Throwable {
    // given
    final var entryCount = 128;

    // when
    raftRule.awaitAppendEntries(entryCount);

    // then
    raftRule.awaitCommit(entryCount + 1);
    raftRule.awaitSameLogSizeOnAllNodes();
    final var memberLog = raftRule.getMemberLog();

    final var logLength = memberLog.values().stream().map(List::size).findFirst().orElseThrow();
    assertThat(logLength).isEqualTo(128);
    assertMemberLogs(memberLog);
  }

  private void assertMemberLogs(final Map<String, List<Indexed<?>>> memberLog) {
    final var firstMemberEntries = memberLog.get("1");
    final var members = memberLog.keySet();
    for (final var member : members) {
      if (!member.equals("1")) {
        final var otherEntries = memberLog.get(member);

        assertThat(firstMemberEntries).containsExactly(otherEntries.toArray(new Indexed[0]));
      }
    }
  }
}
