package org.hyperledger.besu.ethereum.p2p.subnode;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.plugin.data.EnodeURL;

public class SubnodePeer implements Peer {
    private final String peerName;

    public SubnodePeer(String peerName) {
        this.peerName = peerName;
    }

    public String getPeerName() {
        return peerName;
    }

    @Override
    public EnodeURL getEnodeURL() {
        return null;
    }

    @Override
    public Bytes getId() {
        return null;
    }

    @Override
    public Bytes32 keccak256() {
        return null;
    }
}
