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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.raft.RaftServer.Builder;
import io.atomix.raft.RaftServer.Role;
import io.atomix.raft.cluster.RaftMember;
import io.atomix.raft.partition.impl.RaftNamespaces;
import io.atomix.raft.primitive.TestMember;
import io.atomix.raft.protocol.TestRaftProtocolFactory;
import io.atomix.raft.protocol.TestRaftServerProtocol;
import io.atomix.raft.roles.LeaderRole;
import io.atomix.raft.storage.RaftStorage;
import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.raft.zeebe.ZeebeEntry;
import io.atomix.raft.zeebe.ZeebeLogAppender;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.journal.Indexed;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.LoggerFactory;

public class RaftRule extends ExternalResource {

  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private volatile int nextId;
  private volatile List<RaftMember> members;
  private Map<String, List<Indexed<?>>> memberLog;
  private final List<RaftServer> servers = new ArrayList<>();
  private volatile TestRaftProtocolFactory protocolFactory;
  private volatile ThreadContext context;
  private Path directory;
  private final Map<MemberId, TestRaftServerProtocol> serverProtocols = Maps.newConcurrentMap();
  private final int nodeCount;
  private volatile long highestCommit;
  private final AtomicReference<CommitAwaiter> commitAwaiterRef = new AtomicReference<>();

  public RaftRule(final int nodeCount) {
    this.nodeCount = nodeCount;
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    final var statement = super.apply(base, description);
    return temporaryFolder.apply(statement, description);
  }

  @Override
  protected void before() throws Throwable {
    LoggerFactory.getLogger("test").error("before");
    directory = temporaryFolder.newFolder().toPath();

    members = new ArrayList<>();
    memberLog = new ConcurrentHashMap<>();
    nextId = 0;
    context = new SingleThreadContext("raft-test-messaging-%d");
    protocolFactory = new TestRaftProtocolFactory(context);

    createServers(nodeCount);
  }

  @Override
  protected void after() {
    LoggerFactory.getLogger("test").error("after");
    servers.forEach(
        s -> {
          try {
            if (s.isRunning()) {
              s.shutdown().get(10, TimeUnit.SECONDS);
            }
          } catch (final Exception e) {
            // its fine..
          }
        });

    if (context != null) {
      context.close();
    }

    members.clear();
    nextId = 0;
    servers.clear();
    context = null;
    protocolFactory = null;
    highestCommit = 0;
    commitAwaiterRef.set(null);
  }

  /**
   * Returns the next server address.
   *
   * @param type The startup member type.
   * @return The next server address.
   */
  private RaftMember nextMember(final RaftMember.Type type) {
    return new TestMember(nextNodeId(), type);
  }

  /**
   * Returns the next unique member identifier.
   *
   * @return The next unique member identifier.
   */
  private MemberId nextNodeId() {
    return MemberId.from(String.valueOf(++nextId));
  }

  /** Creates a set of Raft servers. */
  private List<RaftServer> createServers(final int nodes) throws Exception {
    final List<RaftServer> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMember(RaftMember.Type.ACTIVE));
    }

    final CountDownLatch latch = new CountDownLatch(nodes);

    for (int i = 0; i < nodes; i++) {
      final RaftServer server = createServer(members.get(i).memberId());
      if (members.get(i).getType() == RaftMember.Type.ACTIVE) {
        server
            .bootstrap(members.stream().map(RaftMember::memberId).collect(Collectors.toList()))
            .thenAccept(this::addCommitListener)
            .thenRun(latch::countDown);
      } else {
        server
            .listen(members.stream().map(RaftMember::memberId).collect(Collectors.toList()))
            .thenAccept(this::addCommitListener)
            .thenRun(latch::countDown);
      }
      servers.add(server);
    }

    latch.await(30 * nodes, TimeUnit.SECONDS);

    return servers;
  }

  public void shutdownServer(final String memberId) {
    servers.stream()
        .filter(server -> server.name().equals(memberId))
        .findFirst()
        .orElseThrow()
        .shutdown()
        .join();
  }

  public void awaitNewLeader() {
    waitUntil(() -> getLeader().isPresent(), 100);
  }

  private void addCommitListener(final RaftServer raftServer) {
    memberLog.put(raftServer.name(), new CopyOnWriteArrayList<>());
    raftServer
        .getContext()
        .addCommitListener(
            new RaftCommitListener() {
              @Override
              public <T extends RaftLogEntry> void onCommit(final Indexed<T> entry) {
                memberLog.get(raftServer.name()).add(entry);

                final var index = entry.index();
                if (highestCommit < index) {
                  highestCommit = index;
                }

                final var commitAwaiter = commitAwaiterRef.get();
                if (commitAwaiter != null && commitAwaiter.reachedCommit(index)) {
                  commitAwaiterRef.set(null);
                }
              }
            });
  }

  public Map<String, List<Indexed<?>>> getMemberLog() {
    return memberLog;
  }

  public void awaitSameLogSizeOnAllNodes() {
    waitUntil(() -> memberLog.values().stream().map(List::size).distinct().count() == 1);
  }

  private void waitUntil(final BooleanSupplier condition) {
    waitUntil(condition, 1000);
  }

  private void waitUntil(final BooleanSupplier condition, int retries) {
    try {
      while (!condition.getAsBoolean() && retries > 0) {
        Thread.sleep(100);
        retries--;
      }
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    assertTrue(condition.getAsBoolean());
  }

  public void awaitCommit(final long commitIndex) throws Exception {
    if (highestCommit >= commitIndex) {
      return;
    }

    final var commitAwaiter = new CommitAwaiter(commitIndex);
    commitAwaiterRef.set(commitAwaiter);

    commitAwaiter.awaitCommit();
  }

  /** Creates a Raft server. */
  private RaftServer createServer(final MemberId memberId) {
    return createServer(memberId, b -> b.withStorage(createStorage(memberId)));
  }

  private RaftServer createServer(
      final MemberId memberId, final Function<Builder, Builder> configurator) {
    final TestRaftServerProtocol protocol = protocolFactory.newServerProtocol(memberId);
    final RaftServer.Builder defaults =
        RaftServer.builder(memberId)
            .withMembershipService(mock(ClusterMembershipService.class))
            .withProtocol(protocol);
    final RaftServer server = configurator.apply(defaults).build();

    serverProtocols.put(memberId, protocol);
    servers.add(server);
    return server;
  }

  private RaftStorage createStorage(final MemberId memberId) {
    return createStorage(memberId, Function.identity());
  }

  private RaftStorage createStorage(
      final MemberId memberId,
      final Function<RaftStorage.Builder, RaftStorage.Builder> configurator) {
    final RaftStorage.Builder defaults =
        RaftStorage.builder()
            .withStorageLevel(StorageLevel.DISK)
            .withDirectory(new File(directory.toFile(), memberId.toString()))
            .withMaxEntriesPerSegment(10)
            .withMaxSegmentSize(1024 * 10)
            .withNamespace(RaftNamespaces.RAFT_STORAGE);
    return configurator.apply(defaults).build();
  }

  private Optional<RaftServer> getLeader() {
    return servers.stream().filter(s -> s.getRole() == Role.LEADER).findFirst();
  }

  public void appendEntries(final int count) {
    final var leader = getLeader().orElseThrow();

    for (int i = 0; i < count; i++) {
      appendEntryAsync(leader, 1024);
    }
  }

  public long appendEntry() throws Exception {
    final var leader = getLeader().orElseThrow();

    return appendEntry(leader, 1024);
  }

  private long appendEntry(final RaftServer leader, final int entrySize) throws Exception {
    final var raftRole = leader.getContext().getRaftRole();
    if (raftRole instanceof LeaderRole) {
      final var leaderRole = (LeaderRole) raftRole;
      final var appendListener = new TestAppendListener();
      leaderRole.appendEntry(
          0, 10, ByteBuffer.wrap(RandomStringUtils.random(entrySize).getBytes()), appendListener);
      return appendListener.awaitCommit();
    }
    throw new IllegalArgumentException(
        "Expected to append entry on leader, "
            + leader.getContext().getName()
            + " was not the leader!");
  }

  private void appendEntryAsync(final RaftServer leader, final int entrySize) {
    final var raftRole = leader.getContext().getRaftRole();

    if (raftRole instanceof LeaderRole) {
      final var leaderRole = (LeaderRole) raftRole;
      final var appendListener = new TestAppendListener();
      leaderRole.appendEntry(
          0, 10, ByteBuffer.wrap(RandomStringUtils.random(entrySize).getBytes()), appendListener);
      return;
    }

    throw new IllegalArgumentException(
        "Expected to append entry on leader, "
            + leader.getContext().getName()
            + " was not the leader!");
  }

  public void awaitAppendEntries(final int i) throws Exception {
    // this call is async
    appendEntries(i - 1);

    // this awaits the last append
    appendEntry();
  }

  @Override
  public String toString() {
    return "RaftRule with " + nodeCount + " nodes.";
  }

  private static final class CommitAwaiter {

    private final long awaitedIndex;
    private final CountDownLatch latch = new CountDownLatch(1);

    public CommitAwaiter(final long index) {
      this.awaitedIndex = index;
    }

    public boolean reachedCommit(final long index) {
      if (this.awaitedIndex <= index) {
        latch.countDown();
        return true;
      }
      return false;
    }

    public void awaitCommit() throws Exception {
      latch.await(30, TimeUnit.SECONDS);
    }
  }

  private static final class TestAppendListener implements ZeebeLogAppender.AppendListener {

    private final CompletableFuture<Long> commitFuture = new CompletableFuture<>();

    @Override
    public void onWrite(final Indexed<ZeebeEntry> indexed) {}

    @Override
    public void onWriteError(final Throwable error) {
      fail("Unexpected write error: " + error.getMessage());
    }

    @Override
    public void onCommit(final Indexed<ZeebeEntry> indexed) {
      commitFuture.complete(indexed.index());
    }

    @Override
    public void onCommitError(final Indexed<ZeebeEntry> indexed, final Throwable error) {
      fail("Unexpected write error: " + error.getMessage());
    }

    public long awaitCommit() throws Exception {
      return commitFuture.get(30, TimeUnit.SECONDS);
    }
  }
}
