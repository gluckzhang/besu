package org.hyperledger.besu.ethereum.p2p.subnode;

import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.AbstractPeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;

public class SubnodePeerConnection extends AbstractPeerConnection {
    public SubnodePeerConnection(Peer peer, PeerInfo peerInfo, String connectionId) {
        super(
            peer,
            peerInfo,
            null,
            null,
            connectionId,
            null,
            null,
            null);
    }

    @Override
    protected void doSendMessage(Capability capability, MessageData message) {

    }

    @Override
    protected void closeConnectionImmediately() {

    }

    @Override
    protected void closeConnection() {

    }
}
