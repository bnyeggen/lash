package com.nyeggen.lash.serde;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class IntSerde implements Serde<Integer>{
	private IntSerde(){}
	private static final IntSerde inst = new IntSerde();
	public static IntSerde getInstance(){ return inst; }

	@Override
	public Integer fromBytes(byte[] b) {
		if (b == null || b.length == 0) return null;
		final ByteBuffer buf = ByteBuffer.wrap(b);
		buf.order(ByteOrder.nativeOrder());
		return buf.getInt();
	}
	@Override
	public byte[] toBytes(Integer t) {
		if(t == null) return new byte[]{};
		final ByteBuffer buf = ByteBuffer.allocate(4);
		buf.order(ByteOrder.nativeOrder());
		buf.putInt(t);
		return buf.array();
	}
}
