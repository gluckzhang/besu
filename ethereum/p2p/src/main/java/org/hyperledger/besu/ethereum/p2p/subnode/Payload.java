package org.hyperledger.besu.ethereum.p2p.subnode;

public class Payload {
    private final int Code;
    private final int Size;
    private final String Payload;

    public Payload(int code, int size, String payload) {
        Code = code;
        Size = size;
        Payload = payload;
    }

    public int getCode() {
        return Code;
    }

    public int getSize() {
        return Size;
    }

    public String getPayload() {
        return Payload;
    }
}
