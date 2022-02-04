package org.hyperledger.besu.ethereum.p2p.subnode;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.AbstractPeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SubnodePeerConnection implements PeerConnection {
    private static final Logger LOG = LogManager.getLogger();

    static final AtomicInteger connectionId = new AtomicInteger(0);
    private final SubnodePeer peer;
    private final PeerInfo peerInfo;
    private final Set<Capability> agreedCapabilities;
    private final AtomicBoolean disconnected = new AtomicBoolean(false);
    private Optional<DisconnectMessage.DisconnectReason> disconnectReason = Optional.empty();

    public SubnodePeerConnection(SubnodePeer peer, PeerInfo peerInfo) {
        this.peer = peer;
        this.peerInfo = peerInfo;
        // TODO: the capability info could be added into a queue message (e.g., add_peer queue message)
        this.agreedCapabilities = new HashSet<>();
        this.agreedCapabilities.add(Capability.create("eth", 66));
    }

    @Override
    public void send(Capability capability, MessageData message) throws PeerNotConnected {
        String exchangeName = this.peer.getPeerName() + "-out";

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        Payload payload = new Payload(message.getCode(), message.getSize(), new String(message.getData().toArray()));
        try {
            RabbitmqAgent.sendMessage(exchangeName, gson.toJson(payload));
            LOG.info("SubnodePeer successfully sended message {}, {}", new String(message.getData().toArray()), capability);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Set<Capability> getAgreedCapabilities() {
        return agreedCapabilities;
    }

    @Override
    public Peer getPeer() {
        return peer;
    }

    @Override
    public PeerInfo getPeerInfo() {
        return peerInfo;
    }

    @Override
    public void terminateConnection(DisconnectMessage.DisconnectReason reason, boolean peerInitiated) {
        if (disconnected.compareAndSet(false, true)) {
            // do nothing for now
        }
    }

    @Override
    public void disconnect(DisconnectMessage.DisconnectReason reason) {
        if (disconnected.compareAndSet(false, true)) {
            // do nothing for now
        }
    }

    @Override
    public boolean isDisconnected() {
        return disconnected.get();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return null;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return null;
    }
}
