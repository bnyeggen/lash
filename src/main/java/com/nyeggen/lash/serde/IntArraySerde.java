package com.nyeggen.lash.serde;

import com.nyeggen.lash.util.MMapper;

@SuppressWarnings("restriction")
public class IntArraySerde implements Serde<int[]> {
	private IntArraySerde(){}
	private static final IntArraySerde inst = new IntArraySerde();
	public static IntArraySerde getInstance(){ return inst; }
	
	@Override
	public int[] fromBytes(byte[] d) {
		if(d == null) return null;
		if(d.length == 0) return new int[]{};
		final int len = d.length;
		final int[] out = new int[len >> 2];
        MMapper.getUnsafe().copyMemory(
        		d, MMapper.getByteArrayOffset(),
                out, MMapper.getIntArrayOffset(),
                len);
		return out;

	}
	@Override
	public byte[] toBytes(int[] t) {
		if(t==null || t.length == 0) return new byte[]{};
		if(t.length > Integer.MAX_VALUE >> 2) 
			throw new IllegalArgumentException("Array is too big to serialize");
		final int len = t.length << 2;
		final byte[] out = new byte[len];
		MMapper.getUnsafe().copyMemory(
				t, MMapper.getIntArrayOffset(),
                out, MMapper.getByteArrayOffset(),
                len);
		return out;
	}
	
}
