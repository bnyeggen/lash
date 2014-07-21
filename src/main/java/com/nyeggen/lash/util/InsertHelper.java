package com.nyeggen.lash.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

import com.nyeggen.lash.AbstractDiskMap;

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

	public static final Runnable makeLongInserter(final AtomicLong ctr, final AbstractDiskMap dmap, final long start, final long end) {
		return new Runnable() {
			public void run() {
				for (long i = start; i < end; i++) {
					final long curCtr = ctr.incrementAndGet();
					if (curCtr % 1000000 == 0)
						System.out.println("Inserted " + curCtr + " records");
					dmap.putIfAbsent(longToBytes(i), longToBytes(i + 1));
				}
			}
		};
	}
}