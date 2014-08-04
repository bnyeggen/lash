package com.nyeggen.lash.serde;

import com.nyeggen.lash.util.MMapper;

@SuppressWarnings("restriction")
public class IntSerde implements Serde<Integer>{
	private IntSerde(){}
	private static final IntSerde inst = new IntSerde();
	public static IntSerde getInstance(){ return inst; }

	@Override
	public Integer fromBytes(byte[] b) {
		if (b == null || b.length == 0) return null;
		return MMapper.getUnsafe().getInt(b, MMapper.getByteArrayOffset());
	}
	@Override
	public byte[] toBytes(Integer t) {
		if(t == null) return new byte[]{};
		final byte[] out = new byte[4];
		MMapper.getUnsafe().putInt(out, MMapper.getByteArrayOffset(), t.intValue());
		return out;
	}
}
