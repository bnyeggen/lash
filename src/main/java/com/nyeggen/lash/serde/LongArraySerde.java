package com.nyeggen.lash.serde;

import com.nyeggen.lash.util.MMapper;

@SuppressWarnings("restriction")
public class LongArraySerde implements Serde<long[]> {
	private LongArraySerde(){}
	private static final LongArraySerde inst = new LongArraySerde();
	public static LongArraySerde getInstance(){ return inst; }
	
	@Override
	public long[] fromBytes(byte[] d) {
		if(d == null) return null;
		if(d.length == 0) return new long[]{};
		final int len = d.length;
		final long[] out = new long[len >> 3];
        MMapper.getUnsafe().copyMemory(
        		d, MMapper.getByteArrayOffset(),
                out, MMapper.getLongArrayOffset(),
                len);
		return out;
	}
	@Override
	public byte[] toBytes(long[] t) {
		if(t==null || t.length == 0) return new byte[]{};
		if(t.length > Integer.MAX_VALUE >> 3) 
			throw new IllegalArgumentException("Array is too big to serialize");
		final int len = t.length << 3;
		final byte[] out = new byte[len];
		MMapper.getUnsafe().copyMemory(
				t, MMapper.getLongArrayOffset(),
                out, MMapper.getByteArrayOffset(),
                len);
		return out;
	}
}
