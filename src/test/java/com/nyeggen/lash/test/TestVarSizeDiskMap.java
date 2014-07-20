package com.nyeggen.lash.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.nyeggen.lash.AbstractDiskMap;
import com.nyeggen.lash.VarSizeDiskMap;
import com.nyeggen.lash.util.MMapper;

@RunWith(JUnit4.class)
public class TestVarSizeDiskMap {

	@Test
	public void testConcurrentInserts() throws Exception {
		final File tmpDir = MMapper.createTempDir();
		final String dir = tmpDir.getCanonicalPath();
		final AbstractDiskMap dmap = new VarSizeDiskMap(dir, 0);
		final AtomicLong ctr = new AtomicLong(0);
		final int recsPerThread = 750000;
		try {
			final Thread t1 = new Thread(Helper.makeLongInserter(ctr, dmap, recsPerThread*0, recsPerThread*1));
			final Thread t2 = new Thread(Helper.makeLongInserter(ctr, dmap, recsPerThread*1, recsPerThread*2));
			final Thread t3 = new Thread(Helper.makeLongInserter(ctr, dmap, recsPerThread*2, recsPerThread*3));
			final Thread t4 = new Thread(Helper.makeLongInserter(ctr, dmap, recsPerThread*3, recsPerThread*4));

			t1.start(); t2.start(); t3.start(); t4.start();
			t1.join();  t2.join();  t3.join();  t4.join();
			for(long i=0; i<recsPerThread*4; i++){
				assertEquals(Helper.bytesToLong(dmap.get(Helper.longToBytes(i))), i+1);
			}
		} finally {
			dmap.delete();
		}
	}
}
