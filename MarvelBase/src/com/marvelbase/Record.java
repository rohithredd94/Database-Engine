package com.marvelbase;

import java.io.IOException;
import java.io.RandomAccessFile;

public class Record {
	private byte[] bytes = null;
	public Record(RandomAccessFile file, long pointer, boolean isLeaf) throws IOException {
		file.seek(pointer);
		int bytesLength = 8;
		if(isLeaf) {
			bytesLength = file.readShort() + 6;
			file.seek(pointer);
		}
		bytes = new byte[bytesLength];
		file.read(bytes, 0, bytesLength);
	}

	public Record(byte[] bytes) {
		this.bytes = bytes;
	}
	
	public byte[] getBytes() {
		return bytes;
	}

	public int getRecordLength() {
		return bytes.length;
	}
}
