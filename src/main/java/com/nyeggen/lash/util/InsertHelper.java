package com.nyeggen.lash.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class InsertHelper {
	public static final byte[] longToBytes(long i) {
		final ByteBuffer buf = ByteBuffer.allocate(8);
		buf.order(ByteOrder.nativeOrder());
		buf.putLong(i);
		return buf.array();
	}

	public static final long bytesToLong(byte[] b) {
		if (b == null)
			return -1;
		final ByteBuffer buf = ByteBuffer.wrap(b);
		buf.order(ByteOrder.nativeOrder());
		return buf.getLong();
	}
}