package com.nyeggen.lash.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.nyeggen.lash.AbstractDiskMap;
import com.nyeggen.lash.VarSizeDiskMap;
import com.nyeggen.lash.util.MMapper;

@RunWith(JUnit4.class)
public class TestAll {
	private static final byte[] longToBytes(long i){
		final ByteBuffer buf = ByteBuffer.allocate(8);
		buf.order(ByteOrder.nativeOrder());
		buf.putLong(i);
		return buf.array();
	}
	private static final long bytesToLong(byte[] b){
		if(b == null) return -1;
		final ByteBuffer buf = ByteBuffer.wrap(b);
		buf.order(ByteOrder.nativeOrder());
		return buf.getLong();
	}
	private static final Runnable makeLongInserter(final AtomicLong ctr, final AbstractDiskMap dmap, final long start, final long end){
		return new Runnable(){
			public void run() {
				for(long i=start; i<end; i++){
					final long curCtr = ctr.incrementAndGet();
					if(curCtr % 1000000 == 0) 
						System.out.println("Inserted " + curCtr + " records");
					dmap.putIfAbsent(longToBytes(i), longToBytes(i+1));
				}
			}
		};
	}

	@Test
	public void testConcurrentInserts() throws Exception {
		final File tmpDir = MMapper.createTempDir();
		final String dir = tmpDir.getCanonicalPath();
		final AbstractDiskMap dmap = new VarSizeDiskMap(dir, 0);
		final AtomicLong ctr = new AtomicLong(0);
		try {
			final Thread t1 = new Thread(makeLongInserter(ctr, dmap, 0       , 7500000));
			final Thread t2 = new Thread(makeLongInserter(ctr, dmap, 7500000 , 15000000));
			final Thread t3 = new Thread(makeLongInserter(ctr, dmap, 15000000, 22500000));
			final Thread t4 = new Thread(makeLongInserter(ctr, dmap, 22500000, 30000000));

			t1.start(); t2.start(); t3.start(); t4.start();
			t1.join();  t2.join();  t3.join();  t4.join();
			for(long i=0; i<30000000; i++){
				assertEquals(bytesToLong(dmap.get(longToBytes(i))), i+1);
			}
		} finally {
			dmap.delete();
		}
	}
}
