package com.nyeggen.lash.test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.nyeggen.lash.AbstractDiskMap;
import com.nyeggen.lash.BucketDiskMap;
import com.nyeggen.lash.DiskMap;
import com.nyeggen.lash.serde.IntSerde;
import com.nyeggen.lash.util.MMapper;

@RunWith(JUnit4.class)
public class TestDiskMap {
	@Test
	public void testSerdes() throws Exception{
		final File tmpDir = MMapper.createTempDir();
		final String dir = tmpDir.getCanonicalPath();
		final AbstractDiskMap backing = new BucketDiskMap(dir);
		final DiskMap<Integer, Integer> dmap = new DiskMap<Integer, Integer>(IntSerde.getInstance(), IntSerde.getInstance(), backing);
		
		final ConcurrentHashMap<Integer, Integer> hmap = new ConcurrentHashMap<Integer, Integer>();
		final Random rng = new Random();
		
		//10M tries, 1M keys
		for(int i=0; i<10000000; i++){
			final int k = rng.nextInt(1000000);
			final int v = rng.nextInt(1000000);
			switch (rng.nextInt(4)){
			case 0:
				dmap.put(k, v);
				hmap.put(k, v);
				break;
			case 1:
				dmap.remove(k);
				hmap.remove(k);
				break;
			case 2:
				dmap.replace(k, v);
				hmap.replace(k, v);
				break;
			case 3:
				dmap.putIfAbsent(k, v);
				hmap.putIfAbsent(k, v);
				break;
			}
		}
		
		for(final Integer i: dmap.keySet()){
			assertEquals(dmap.get(i), hmap.get(i));
		}
		backing.delete();
	}
}
