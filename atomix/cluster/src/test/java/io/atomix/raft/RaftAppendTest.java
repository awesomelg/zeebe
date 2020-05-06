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

  @Parameters(name = "{index}: {0}")
  public static Object[][] reprocessingTriggers() {
    return new Object[][] {
        new Object[] {
            new RaftRule(2)
        },
        new Object[] {
            new RaftRule(3)
        }
        ,
        new Object[] {
            new RaftRule(4)
        }
        ,
        new Object[] {
            new RaftRule(5)
        }
    };
  }

  @Rule
  @Parameter
  public RaftRule raftRule;


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
    for (final var member : members)
    {
      if (!member.equals("1")) {
        final var otherEntries = memberLog.get(member);

        assertThat(firstMemberEntries).containsExactly(otherEntries.toArray(new Indexed[0]));
      }
    }
  }
}
