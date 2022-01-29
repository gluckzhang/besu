/*
 * Copyright ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.p2p.network;

import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.discovery.DNSDaemon;
import org.hyperledger.besu.crypto.NodeKey;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeer;
import org.hyperledger.besu.ethereum.p2p.peers.*;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionsDenylist;
import org.hyperledger.besu.ethereum.p2p.rlpx.ConnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.DisconnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.MessageCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.netty.TLSConfiguration;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.subnode.RabbitmqAgent;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * N-version Ethereum network service for a sub-node
 */
public class SubnodeNetwork implements P2PNetwork {

  private static final Logger LOG = LogManager.getLogger();

  private final ScheduledExecutorService peerConnectionScheduler =
      Executors.newSingleThreadScheduledExecutor();
  private final RabbitmqAgent rabbitmqAgent;

  private final NetworkingConfiguration config;

  private final Bytes nodeId;
  private final MutableLocalNode localNode;

  private final PeerPermissions peerPermissions;
  private final MaintainedPeers maintainedPeers;

  private OptionalLong peerBondedObserverId = OptionalLong.empty();

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final CountDownLatch shutdownLatch = new CountDownLatch(2);
  private final Duration shutdownTimeout = Duration.ofMinutes(1);

  private final AtomicReference<List<DiscoveryPeer>> dnsPeers = new AtomicReference<>();
  private DNSDaemon dnsDaemon;

  /**
   * Creates a peer networking service for production purposes.
   *
   * <p>The caller is expected to provide the IP address to be advertised (normally this node's
   * public IP address), as well as TCP and UDP port numbers for the RLPx agent and the discovery
   * agent, respectively.
   *
   * @param localNode A representation of the local node
   * @param rabbitmqAgent The agent responsible for interacting with RabbitMQ.
   * @param nodeKey The node key through which cryptographic operations can be performed
   * @param config The network configuration to use.
   * @param peerPermissions An object that determines whether peers are allowed to connect
   * @param maintainedPeers A collection of peers for which we are expected to maintain connections
   * @param reputationManager An object that inspect disconnections for misbehaving peers that can
   *     then be blacklisted.
   */
  SubnodeNetwork(
      final MutableLocalNode localNode,
      final RabbitmqAgent rabbitmqAgent,
      final NodeKey nodeKey,
      final NetworkingConfiguration config,
      final PeerPermissions peerPermissions,
      final MaintainedPeers maintainedPeers,
      final PeerReputationManager reputationManager) {
    this.localNode = localNode;
    this.rabbitmqAgent = rabbitmqAgent;
    this.config = config;
    this.maintainedPeers = maintainedPeers;

    this.nodeId = nodeKey.getPublicKey().getEncodedBytes();
    this.peerPermissions = peerPermissions;

    final int maxPeers = config.getRlpx().getMaxPeers();
    subscribeDisconnect(reputationManager);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void start() {
    if (!started.compareAndSet(false, true)) {
      LOG.warn("Attempted to start an already started " + getClass().getSimpleName());
      return;
    }
    rabbitmqAgent.start();
  }

  @Override
  public void stop() {

  }

  @Override
  public void awaitStop() {

  }

  @Override
  public boolean addMaintainConnectionPeer(final Peer peer) {
    final boolean wasAdded = true;
    return wasAdded;
  }

  @Override
  public boolean removeMaintainedConnectionPeer(final Peer peer) {
    final boolean wasRemoved = true;
    return wasRemoved;
  }

  @Override
  public Collection<PeerConnection> getPeers() {
    return null;
  }

  @Override
  public Stream<DiscoveryPeer> streamDiscoveredPeers() {
    return Stream.empty();
  }

  @Override
  public CompletableFuture<PeerConnection> connect(final Peer peer) {
    return null;
  }

  @Override
  public void subscribe(final Capability capability, final MessageCallback callback) {
  }

  @Override
  public void subscribeConnect(final ConnectCallback callback) {
  }

  @Override
  public void subscribeDisconnect(final DisconnectCallback callback) {
  }

  @Override
  public void close() {
    stop();
  }

  @Override
  public boolean isListening() {
    return localNode.isReady();
  }

  @Override
  public boolean isP2pEnabled() {
    return true;
  }

  @Override
  public boolean isDiscoveryEnabled() {
    return false;
  }

  @Override
  public Optional<EnodeURL> getLocalEnode() {
    if (!localNode.isReady()) {
      return Optional.empty();
    }
    return Optional.of(localNode.getPeer().getEnodeURL());
  }

  @Override
  public void updateNodeRecord() {
  }

  public static class Builder {

    private Vertx vertx;
    private RabbitmqAgent rabbitmqAgent;

    private NetworkingConfiguration config = NetworkingConfiguration.create();
    private List<Capability> supportedCapabilities;
    private NodeKey nodeKey;

    private MaintainedPeers maintainedPeers = new MaintainedPeers();
    private PeerPermissions peerPermissions = PeerPermissions.noop();

    private MetricsSystem metricsSystem;
    private StorageProvider storageProvider;
    private Supplier<List<Bytes>> forkIdSupplier;
    private Optional<TLSConfiguration> p2pTLSConfiguration = Optional.empty();

    public P2PNetwork build() {
      validate();
      return doBuild();
    }

    private P2PNetwork doBuild() {
      // Set up permissions
      // Fold peer reputation into permissions
      final PeerPermissionsDenylist misbehavingPeers = PeerPermissionsDenylist.create(500);
      final PeerReputationManager reputationManager = new PeerReputationManager(misbehavingPeers);
      peerPermissions = PeerPermissions.combine(peerPermissions, misbehavingPeers);

      final MutableLocalNode localNode =
          MutableLocalNode.create(config.getRlpx().getClientId(), 5, supportedCapabilities);
      final PeerPrivileges peerPrivileges = new DefaultPeerPrivileges(maintainedPeers);
      rabbitmqAgent = rabbitmqAgent == null ? createRabbitmqAgent(localNode, metricsSystem) : rabbitmqAgent;

      return new SubnodeNetwork(
          localNode,
          rabbitmqAgent,
          nodeKey,
          config,
          peerPermissions,
          maintainedPeers,
          reputationManager);
    }

    private void validate() {
      checkState(nodeKey != null, "NodeKey must be set.");
      checkState(config != null, "NetworkingConfiguration must be set.");
      checkState(
          supportedCapabilities != null && supportedCapabilities.size() > 0,
          "Supported capabilities must be set and non-empty.");
      checkState(metricsSystem != null, "MetricsSystem must be set.");
      checkState(storageProvider != null, "StorageProvider must be set.");
      checkState(forkIdSupplier != null, "ForkIdSupplier must be set.");
    }

    private RabbitmqAgent createRabbitmqAgent(LocalNode localNode, MetricsSystem metricsSystem) {
      return new RabbitmqAgent(localNode, metricsSystem);
    }

    public Builder rabbitmqAgent(final RabbitmqAgent rabbitmqAgent) {
      checkNotNull(rabbitmqAgent);
      this.rabbitmqAgent = rabbitmqAgent;
      return this;
    }

    public Builder vertx(final Vertx vertx) {
      checkNotNull(vertx);
      this.vertx = vertx;
      return this;
    }

    public Builder nodeKey(final NodeKey nodeKey) {
      checkNotNull(nodeKey);
      this.nodeKey = nodeKey;
      return this;
    }

    public Builder config(final NetworkingConfiguration config) {
      checkNotNull(config);
      this.config = config;
      return this;
    }

    public Builder supportedCapabilities(final List<Capability> supportedCapabilities) {
      checkNotNull(supportedCapabilities);
      this.supportedCapabilities = supportedCapabilities;
      return this;
    }

    public Builder supportedCapabilities(final Capability... supportedCapabilities) {
      this.supportedCapabilities = Arrays.asList(supportedCapabilities);
      return this;
    }

    public Builder peerPermissions(final PeerPermissions peerPermissions) {
      checkNotNull(peerPermissions);
      this.peerPermissions = peerPermissions;
      return this;
    }

    public Builder metricsSystem(final MetricsSystem metricsSystem) {
      checkNotNull(metricsSystem);
      this.metricsSystem = metricsSystem;
      return this;
    }

    public Builder maintainedPeers(final MaintainedPeers maintainedPeers) {
      checkNotNull(maintainedPeers);
      this.maintainedPeers = maintainedPeers;
      return this;
    }

    public Builder storageProvider(final StorageProvider storageProvider) {
      checkNotNull(storageProvider);
      this.storageProvider = storageProvider;
      return this;
    }

    public Builder forkIdSupplier(final Supplier<List<Bytes>> forkIdSupplier) {
      checkNotNull(forkIdSupplier);
      this.forkIdSupplier = forkIdSupplier;
      return this;
    }
  }
}
