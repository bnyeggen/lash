package com.nyeggen.lash.serde;

/**For now we make the assumption that bytewise equality is the way to check
 * for serialized equality (as it is also the way we calculate hash codes for
 * every operation).  If that is not the case, we need to provide a
 * "canonicalize" method as well for deriving consistently hashable and
 * comparable keys.
 * All methods of a Serde must deal with nulls.*/
public interface Serde<T> {
	byte[] toBytes(T t);
	T fromBytes(byte[] d);
}
