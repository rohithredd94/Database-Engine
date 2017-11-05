package com.marvelbase.DataType;

public class TinyInt implements DataType<Byte> {

	private Byte value;

	public TinyInt(Byte value) {
		this.value = value;
	}

	@Override
	public Byte getValue() {
		return this.value;
	}

	@Override
	public boolean equal(Object rightValue) {
		return this.value.equals(rightValue);
	}

	@Override
	public boolean notEqual(Object rightValue) {
		return !this.value.equals(rightValue);
	}

	@Override
	public boolean greater(Object rightValue) {
		return rightValue instanceof Byte && this.value > (Byte) rightValue;
	}

	@Override
	public boolean greaterEquals(Object rightValue) {
		return rightValue instanceof Byte && this.value >=(Byte)  rightValue;
	}

	@Override
	public boolean lesser(Object rightValue) {
		return rightValue instanceof Byte && this.value < (Byte) rightValue;
	}

	@Override
	public boolean lesserEquals(Object rightValue) {
		return rightValue instanceof Byte && this.value <=(Byte)  rightValue;
	}

	@Override
	public boolean like(Object rightValue) {
		return false;
	}

	@Override
	public byte[] getByteValue() {
		byte[] bytes = new byte[1];
		bytes[0] = value;
		return bytes;
	}

	@Override
	public byte getDataTypeOfValue() {
		if(value == null)
			return 0x00;
		else
			return 0x04;
	}
}