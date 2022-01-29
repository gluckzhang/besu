package org.hyperledger.besu.ethereum.p2p.subnode;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hyperledger.besu.ethereum.p2p.peers.LocalNode;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RabbitmqAgent {
    private static final Logger LOG = LogManager.getLogger();

    private final LocalNode localNode;
    private int connectionCount = 0;

    private final ConnectionFactory factory = new ConnectionFactory();
    private ExecutorService peerConnectionHandler = null;

    private final Counter connectedPeersCounter;

    public RabbitmqAgent(LocalNode localNode, MetricsSystem metricsSystem) {
        this.localNode = localNode;

        // Setup metrics
        connectedPeersCounter =
            metricsSystem.createCounter(
                BesuMetricCategory.PEERS, "connected_total", "Total number of peers connected");

        metricsSystem.createIntegerGauge(
            BesuMetricCategory.ETHEREUM,
            "peer_count",
            "The current number of peers connected",
            this::getConnectionCount);
    }

    public void start() {
        this.peerConnectionHandler = subscribePeerConnectionEvents();
    }

    private ExecutorService subscribePeerConnectionEvents() {
        String exchangeName = "add_peer";
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String peerName = new String(delivery.getBody(), "UTF-8");
            LOG.info("new peer connected: {}", peerName);
            connectedPeersCounter.inc();

            String exchangeNameForPeer = peerName + "-in";
            DeliverCallback callback = (c, d) -> {
                String peerMessage = new String(d.getBody(), "UTF-8");
                LOG.info("new message from peer {}: {}", peerName, peerMessage);
                // TODO: handle the peer message here
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

    public static void main(String[] args) { }
}