package com.nyeggen.lash.test;

import java.io.File;

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
		//This fails reliably at 106052 -> 106053
		//But oddly not the testInserts, which just runs in the main thread
		//rather than a single forked thread?
		final File tmpDir = MMapper.createTempDir();
		final String dir = tmpDir.getCanonicalPath();
		final AbstractDiskMap dmap = new BucketDiskMap(dir);
		final int recsPerThread = 2000000;
		try {
			final Thread t1 = new Thread(new Runnable() {
				@Override
				public void run() {
					for(long i=0; i<recsPerThread; i++){
						final byte[] k = InsertHelper.longToBytes(i);
						final byte[] v = InsertHelper.longToBytes(i+1);
						dmap.put(k, v);
					}
				}
			});
			final Thread t2 = new Thread(new Runnable() {
				@Override
				public void run() {
					for(long i=recsPerThread; i<recsPerThread*2; i++){
						final byte[] k = InsertHelper.longToBytes(i);
						final byte[] v = InsertHelper.longToBytes(i+1);
						dmap.put(k, v);
					}
				}
			});
			final Thread t3 = new Thread(new Runnable() {
				@Override
				public void run() {
					for(long i=recsPerThread*2; i<recsPerThread*3; i++){
						final byte[] k = InsertHelper.longToBytes(i);
						final byte[] v = InsertHelper.longToBytes(i+1);
						dmap.put(k, v);
					}
				}
			});
			final Thread t4 = new Thread(new Runnable() {
				@Override
				public void run() {
					for(long i=recsPerThread; i<recsPerThread*4; i++){
						final byte[] k = InsertHelper.longToBytes(i);
						final byte[] v = InsertHelper.longToBytes(i+1);
						dmap.put(k, v);
					}
				}
			});

			t1.start(); t2.start(); t3.start(); t4.start();
			t1.join();  t2.join();  t3.join();  t4.join();
			System.out.println("Checking inserted values...");
			for(long i=0; i<recsPerThread*4; i++){
				final byte[] k = InsertHelper.longToBytes(i);
				final byte[] v = dmap.get(k);
				assertEquals(i+1, InsertHelper.bytesToLong(v));
			}
		} finally {
			dmap.close();
			dmap.delete();
		}
	}

	@Test
	public void testInserts() throws Exception{
		final File tmpDir = MMapper.createTempDir();
		final String dir = tmpDir.getCanonicalPath();
		final AbstractDiskMap dmap = new BucketDiskMap(dir);
		final int recs = 8000000;
		try {
			for(long i=0; i<recs; i++){
				final byte[] k = InsertHelper.longToBytes(i);
				final byte[] v = InsertHelper.longToBytes(i+1);
				dmap.put(k, v);
				final byte[] vOut = dmap.get(k);
				assertEquals("Insert & retrieval mismatch: " + i
					, i+1
					, InsertHelper.bytesToLong(vOut));
			}
			for(long i=0; i<recs; i++){
				final byte[] k = InsertHelper.longToBytes(i);
				final byte[] v = dmap.get(k);
				assertEquals("Insert overwritten: " + i
					, i+1
					, InsertHelper.bytesToLong(v));
			}
		} finally {
			dmap.close();
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
