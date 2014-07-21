package com.nyeggen.lash.test;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.nyeggen.lash.AbstractDiskMap;
import com.nyeggen.lash.VarSizeDiskMap;
import com.nyeggen.lash.util.MMapper;
import com.nyeggen.lash.util.InsertHelper;

@RunWith(JUnit4.class)
public class TestVarSizeDiskMap {

	@Test
	public void testConcurrentInserts() throws Exception {
		final File tmpDir = MMapper.createTempDir();
		final String dir = tmpDir.getCanonicalPath();
		final AbstractDiskMap dmap = new VarSizeDiskMap(dir, 0);
		final int recsPerThread = 750000;
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
					for(long i=recsPerThread*3; i<recsPerThread*4; i++){
						final byte[] k = InsertHelper.longToBytes(i);
						final byte[] v = InsertHelper.longToBytes(i+1);
						dmap.put(k, v);
					}
				}
			});

			t1.start(); t2.start(); t3.start(); t4.start();
			t1.join();  t2.join();  t3.join();  t4.join();
			for(long i=0; i<recsPerThread*4; i++){
				assertEquals(InsertHelper.bytesToLong(dmap.get(InsertHelper.longToBytes(i))), i+1);
			}
		} finally {
			dmap.close();
			dmap.delete();
		}
	}
}
