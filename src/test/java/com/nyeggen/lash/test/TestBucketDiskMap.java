package com.nyeggen.lash.test;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.nyeggen.lash.AbstractDiskMap;
import com.nyeggen.lash.BucketDiskMap;
import com.nyeggen.lash.util.Hash;
import com.nyeggen.lash.util.MMapper;
import com.nyeggen.lash.util.InsertHelper;

@RunWith(JUnit4.class)
public class TestBucketDiskMap {
	@Test
	public void testConcurrentInserts() throws Exception {
		final File tmpDir = MMapper.createTempDir();
		final String dir = tmpDir.getCanonicalPath();
		final AbstractDiskMap dmap = new BucketDiskMap(dir);
		final AtomicLong ctr = new AtomicLong(0);
		final int recsPerThread = 2000000;
		try {
			final Thread t1 = new Thread(InsertHelper.makeLongInserter(ctr, dmap, recsPerThread*0, recsPerThread*1));
			final Thread t2 = new Thread(InsertHelper.makeLongInserter(ctr, dmap, recsPerThread*1, recsPerThread*2));
			final Thread t3 = new Thread(InsertHelper.makeLongInserter(ctr, dmap, recsPerThread*2, recsPerThread*3));
			final Thread t4 = new Thread(InsertHelper.makeLongInserter(ctr, dmap, recsPerThread*3, recsPerThread*4));

			t1.start(); t2.start(); t3.start(); t4.start();
			t1.join();  t2.join();  t3.join();  t4.join();
			System.out.println("Checking inserted values...");
			for(long i=0; i<recsPerThread*4; i++){
				assertEquals(i+1
						, InsertHelper.bytesToLong(dmap.get(InsertHelper.longToBytes(i))));
			}
		} finally {
			dmap.delete();
		}
	}

	@Test
	public void testHashCollisions() throws Exception {
		final File tmpDir = MMapper.createTempDir();
		final String dir = tmpDir.getCanonicalPath();
		final AbstractDiskMap dmap = new BucketDiskMap(dir);
		try {
			final long[] collisions = new long[500];
			collisions[0] = 1;
			for(int idx = 1; idx<collisions.length; idx++){
				collisions[idx] = Hash.findCollision(collisions[idx-1], 16);
			}
			
			for(final long i : collisions){
				dmap.put(InsertHelper.longToBytes(i), InsertHelper.longToBytes(i+1));
			}
			System.out.println("Checking inserted values...");
			for(int idx=0; idx<collisions.length; idx++){
				final long i = collisions[idx];
				assertEquals("Unexpected inequality on index " + idx + "."
					, i+1
					, InsertHelper.bytesToLong(dmap.get(InsertHelper.longToBytes(i))));
			}
		} finally {
			dmap.close();
			dmap.delete();
		}
	}
}
