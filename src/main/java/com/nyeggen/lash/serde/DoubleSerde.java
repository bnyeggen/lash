package com.nyeggen.lash.serde;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DoubleSerde implements Serde<Double> {
	@Override
	public Double fromBytes(byte[] b) {
		if (b == null || b.length == 0) return null;
		final ByteBuffer buf = ByteBuffer.wrap(b);
		buf.order(ByteOrder.nativeOrder());
		return buf.getDouble();
	}
	@Override
	public byte[] toBytes(Double t) {
		if(t == null) return new byte[]{};
		final ByteBuffer buf = ByteBuffer.allocate(8);
		buf.order(ByteOrder.nativeOrder());
		buf.putDouble(t);
		return buf.array();
	}
	private static final DoubleSerde instance = new DoubleSerde();
	public static DoubleSerde getInstance(){ return instance; }
}
