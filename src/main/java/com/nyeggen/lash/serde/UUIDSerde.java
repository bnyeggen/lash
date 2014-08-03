package com.nyeggen.lash.serde;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

public class UUIDSerde implements Serde<UUID> {
	private UUIDSerde(){}
	private static final UUIDSerde inst = new UUIDSerde();
	public static UUIDSerde getInstance(){ return inst; }
	
	@Override
	public UUID fromBytes(byte[] d) {
		final ByteBuffer buf = ByteBuffer.wrap(d);
		buf.order(ByteOrder.nativeOrder());
		return new UUID(buf.getLong(), buf.getLong());
	}
	@Override
	public byte[] toBytes(UUID t) {
		final ByteBuffer buf = ByteBuffer.allocate(16);
		return buf.putLong(t.getMostSignificantBits())
				.putLong(t.getLeastSignificantBits())
				.array();
	}
	
}
