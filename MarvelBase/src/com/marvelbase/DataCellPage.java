package com.marvelbase;

import java.io.IOException;
import java.io.RandomAccessFile;

public class DataCellPage {

	private int leftChildPointer;
	private int key;

	public DataCellPage(RandomAccessFile file, long fpLocation) throws IOException {
		this(file, fpLocation, false);
	}

	public DataCellPage(RandomAccessFile file, long fpLocation, boolean isLeaf) throws IOException {
		file.seek(fpLocation);
		if(!isLeaf) {
			this.leftChildPointer = file.readInt();
			this.key = file.readInt();
		} else {
			file.skipBytes(2);
			this.key = file.readInt();
		}
	}
	
	public int getKey() {
		return key;
	}

	public int getLeftChildPointer() {
		return leftChildPointer;
	}

	
}
