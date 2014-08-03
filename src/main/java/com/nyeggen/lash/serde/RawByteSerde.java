package com.nyeggen.lash.serde;

public class RawByteSerde implements Serde<byte[]> {
	private RawByteSerde(){}
	private static final RawByteSerde inst = new RawByteSerde();
	public static RawByteSerde getInstance(){ return inst; }
	
	@Override
	public byte[] fromBytes(byte[] d) { return d; }
	@Override
	public byte[] toBytes(byte[] t) { return t; }
}
