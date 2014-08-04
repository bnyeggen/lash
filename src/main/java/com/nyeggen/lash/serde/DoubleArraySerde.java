package com.nyeggen.lash.serde;

import com.nyeggen.lash.util.MMapper;

@SuppressWarnings("restriction")
public class DoubleArraySerde implements Serde<double[]> {
	private DoubleArraySerde(){}
	private static final DoubleArraySerde inst = new DoubleArraySerde();
	public static DoubleArraySerde getInstance(){ return inst; }
	
	@Override
	public double[] fromBytes(byte[] d) {
		if(d == null) return null;
		if(d.length == 0) return new double[]{};
		final int len = d.length;
		final double[] out = new double[len >> 3];
        MMapper.getUnsafe().copyMemory(
        		d, MMapper.getByteArrayOffset(),
                out, MMapper.getDoubleArrayOffset(),
                len);
		return out;
	}
	@Override
	public byte[] toBytes(double[] t) {
		if(t==null || t.length == 0) return new byte[]{};
		if(t.length > Integer.MAX_VALUE >> 3) 
			throw new IllegalArgumentException("Array is too big to serialize");
		final int len = t.length << 3;
		final byte[] out = new byte[len];
		MMapper.getUnsafe().copyMemory(
				t, MMapper.getDoubleArrayOffset(),
                out, MMapper.getByteArrayOffset(),
                len);
		return out;
	}
}
