package com.nyeggen.lash.serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**You should expect this to be pretty slow / inefficient, but it is a good
 * fallback implementation.*/
public class SerializingSerde implements Serde<Serializable> {
	private SerializingSerde(){}
	private static final SerializingSerde inst = new SerializingSerde();
	public static SerializingSerde getInstance(){ return inst; }
	
	@Override
	public Serializable fromBytes(byte[] d) {
		final ByteArrayInputStream bis = new ByteArrayInputStream(d);
		ObjectInput in = null;
		try {
		  in = new ObjectInputStream(bis);
		  return (Serializable)in.readObject();
		} catch(Exception e){
			throw new RuntimeException(e);
		} finally {
			try {
				bis.close();
			} catch (Exception e){
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public byte[] toBytes(Serializable t) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(t);
			return bos.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				bos.close();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
