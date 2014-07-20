package com.nyeggen.lash.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.nyeggen.lash.AbstractDiskMap;
import com.nyeggen.lash.BucketDiskMap;
import com.nyeggen.lash.util.MMapper;

@RunWith(JUnit4.class)
public class TestBucketDiskMap {
	@Test
	public void testConcurrentInserts() throws Exception {
		final File tmpDir = MMapper.createTempDir();
		final String dir = tmpDir.getCanonicalPath();
		final AbstractDiskMap dmap = new BucketDiskMap(dir);
		final AtomicLong ctr = new AtomicLong(0);
		try {
			final Thread t1 = new Thread(Helper.makeLongInserter(ctr, dmap, 0       , 750000));
			final Thread t2 = new Thread(Helper.makeLongInserter(ctr, dmap, 750000 , 1500000));
			final Thread t3 = new Thread(Helper.makeLongInserter(ctr, dmap, 1500000, 2250000));
			final Thread t4 = new Thread(Helper.makeLongInserter(ctr, dmap, 2250000, 3000000));

			t1.start(); t2.start(); t3.start(); t4.start();
			t1.join();  t2.join();  t3.join();  t4.join();
			for(long i=0; i<3000000; i++){
				assertEquals(Helper.bytesToLong(dmap.get(Helper.longToBytes(i))), i+1);
			}
		} finally {
			dmap.delete();
		}
	}

}
