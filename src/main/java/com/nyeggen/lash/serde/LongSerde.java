package com.nyeggen.lash.serde;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongSerde implements Serde<Long> {
	private LongSerde(){}
	private static final LongSerde inst = new LongSerde();
	public static LongSerde getInstance(){ return inst; }

	@Override
	public Long fromBytes(byte[] b) {
		if (b == null || b.length == 0) return null;
		final ByteBuffer buf = ByteBuffer.wrap(b);
		buf.order(ByteOrder.nativeOrder());
		return buf.getLong();
	}
	@Override
	public byte[] toBytes(Long t) {
		if(t == null) return new byte[]{};
		final ByteBuffer buf = ByteBuffer.allocate(8);
		buf.order(ByteOrder.nativeOrder());
		buf.putLong(t);
		return buf.array();
	}
}
