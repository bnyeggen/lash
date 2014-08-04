package com.nyeggen.lash.serde;

import com.nyeggen.lash.util.MMapper;

@SuppressWarnings("restriction")
public class DoubleSerde implements Serde<Double> {
	private DoubleSerde(){}
	private static final DoubleSerde instance = new DoubleSerde();
	public static DoubleSerde getInstance(){ return instance; }

	@Override
	public Double fromBytes(byte[] b) {
		if (b == null || b.length == 0) return null;
		return MMapper.getUnsafe().getDouble(b, MMapper.getByteArrayOffset());
	}
	@Override
	public byte[] toBytes(Double t) {
		if(t == null) return new byte[]{};
		final byte[] out = new byte[8];
		MMapper.getUnsafe().putDouble(out, MMapper.getByteArrayOffset(), t.doubleValue());
		return out;
	}
}
