package org.hyperledger.besu.ethereum.p2p.subnode;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.io.Base64;
import org.hyperledger.besu.ethereum.p2p.peers.LocalNode;
import org.hyperledger.besu.ethereum.p2p.rlpx.ConnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.MessageCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.*;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.PongMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.WireMessageCodes;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.util.Subscribers;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class RabbitmqAgent {
    private static final Logger LOG = LogManager.getLogger();

    private final LocalNode localNode;
    private final Subscribers<ConnectCallback> connectSubscribers = Subscribers.create();
    private final Map<Capability, Subscribers<MessageCallback>> messageSubscribers =
            new ConcurrentHashMap<>();
    private int connectionCount = 0;
    private final List<SubProtocol> subProtocols;

    private static final ConnectionFactory factory = new ConnectionFactory();
    private ExecutorService peerConnectionHandler = null;

    private final Counter connectedPeersCounter;
    private final LabelledMetric<Counter> outboundMessagesCounter;

    public RabbitmqAgent(LocalNode localNode, MetricsSystem metricsSystem, List<SubProtocol> subProtocols) {
        this.localNode = localNode;
        this.subProtocols = subProtocols;

        // Setup metrics
        this.connectedPeersCounter = metricsSystem.createCounter(
            BesuMetricCategory.PEERS,
            "connected_total",
            "Total number of peers connected");

        metricsSystem.createIntegerGauge(
            BesuMetricCategory.ETHEREUM,
            "peer_count",
            "The current number of peers connected",
            this::getConnectionCount);

        this.outboundMessagesCounter = metricsSystem.createLabelledCounter(
            BesuMetricCategory.NETWORK,
            "p2p_messages_outbound",
            "Count of each P2P message sent outbound.",
            "protocol",
            "name",
            "code");
    }

    public void start() {
        this.peerConnectionHandler = subscribePeerConnectionEvents();
    }

    public void subscribeMessage(final Capability capability, final MessageCallback callback) {
        messageSubscribers
            .computeIfAbsent(capability, key -> Subscribers.create(true))
            .subscribe(callback);
    }

    public void subscribeConnect(final ConnectCallback callback) {
        connectSubscribers.subscribe(callback);
    }

    public static void sendMessage(String exchangeName, String message) throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException, IOException, TimeoutException {
        factory.setUri("amqp://guest:guest@localhost:5672");
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        channel.exchangeDeclare(exchangeName, "fanout", true);

        channel.basicPublish(exchangeName, "", null, message.getBytes("UTF-8"));
    }

    private ExecutorService subscribePeerConnectionEvents() {
        String exchangeName = "add_peer";
        Capability capEth66 = Capability.create("eth", 66); // currently we only need to support this
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String peerName = new String(delivery.getBody(), "UTF-8");
            LOG.info("new peer connected: {}", peerName);
            connectedPeersCounter.inc();

            SubnodePeer peer = new SubnodePeer(peerName);
            PeerInfo peerInfo = new PeerInfo(
                0,
                peer.getPeerName(),
                Arrays.asList(capEth66),
                0, Bytes.of(peerName.getBytes()));
            CapabilityMultiplexer multiplexer = new CapabilityMultiplexer(
                subProtocols,
                localNode.getPeerInfo().getCapabilities(),
                peerInfo.getCapabilities());
            SubnodePeerConnection peerConnection = new SubnodePeerConnection(peer, peerInfo, multiplexer, outboundMessagesCounter);
            connectSubscribers.forEach(c -> c.onConnect(peerConnection));

            String exchangeNameForPeer = peerName + "-in";
            DeliverCallback callback = (c, d) -> {
                String peerMessage = new String(d.getBody(), "UTF-8");
                LOG.info("new message from peer {}: {}", peerName, peerMessage);
                JsonObject jsonMessage = new GsonBuilder().disableHtmlEscaping().create().fromJson(peerMessage, JsonObject.class);
                final RawMessage rawMessageData = new RawMessage(jsonMessage.get("Code").getAsInt(), Base64.decode(jsonMessage.get("Payload").getAsString()));
                final CapabilityMultiplexer.ProtocolMessage demultiplexed = multiplexer.demultiplex(rawMessageData);
                final MessageData messageData = demultiplexed.getMessage();
                final Message msg = new DefaultMessage(peerConnection, messageData);

                // Handle Wire messages
                if (demultiplexed.getCapability() == null) {
                    switch (messageData.getCode()) {
                        case WireMessageCodes.PING:
                            LOG.info("SubnodePeer Received Wire PING");
                            try {
                                peerConnection.send(null, PongMessage.get());
                            } catch (final PeerConnection.PeerNotConnected peerNotConnected) {
                                // Nothing to do
                            }
                            break;
                        case WireMessageCodes.PONG:
                            LOG.debug("SubnodePeer Received Wire PONG");
                            break;
                        case WireMessageCodes.DISCONNECT:
                            final DisconnectMessage disconnect = DisconnectMessage.readFrom(messageData);
                            DisconnectMessage.DisconnectReason reason = DisconnectMessage.DisconnectReason.UNKNOWN;
                            try {
                                reason = disconnect.getReason();
                                LOG.info(
                                    "Received Wire DISCONNECT ({}) from peer: {}",
                                    reason.name(),
                                    peerConnection.getPeerInfo());
                            } catch (final RLPException e) {
                                LOG.info(
                                    "Received Wire DISCONNECT with invalid RLP. Peer: {}", peerConnection.getPeerInfo());
                            } catch (final Exception e) {
                                LOG.error(
                                    "Received Wire DISCONNECT, but unable to parse reason. Peer: {}",
                                    peerConnection.getPeerInfo(),
                                    e);
                            }
                            peerConnection.terminateConnection(reason, true);
                    }
                    return;
                }
                // Handle ETH messages
                messageSubscribers
                    .getOrDefault(capEth66, Subscribers.none())
                    .forEach(s -> s.onMessage(capEth66, msg));
            };
            RabbitmqHandler peerMessageHandler = new RabbitmqHandler(exchangeNameForPeer, callback);
            ExecutorService exec = Executors.newSingleThreadExecutor();
            exec.execute(peerMessageHandler);
        };
        RabbitmqHandler addPeerHandler = new RabbitmqHandler(exchangeName, deliverCallback);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        exec.execute(addPeerHandler);
        return exec;
    }

    public int getConnectionCount() {
        return this.connectionCount;
    }

    private class RabbitmqHandler implements Runnable {
        private final String exchangeName;
        private final DeliverCallback deliverCallback;

        public RabbitmqHandler(String exchangeName, DeliverCallback deliverCallback) {
            this.exchangeName = exchangeName;
            this.deliverCallback = deliverCallback;
        }

        @Override
        public void run() {
            try {
                factory.setUri("amqp://guest:guest@localhost:5672");
                Connection conn = factory.newConnection();
                Channel channel = conn.createChannel();

                String exchangeName = this.exchangeName;
                channel.exchangeDeclare(exchangeName, "fanout", true);
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, "");
                channel.basicConsume(queueName, true, this.deliverCallback, consumerTag -> { });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}