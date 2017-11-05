package com.marvelbase.Commands;

import com.marvelbase.*;
import com.marvelbase.DataType.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.marvelbase.MarvelBase.logMessage;

public class CommandHelper {
	
	static void displayTableHeader(List<Column> selectColumns) {
		System.out.println("-----------------------------------------------------------------------------------------------------------------------------------------");
		for (Column column : selectColumns) {
			System.out.print("|\t");
			System.out.format(column.getFormat(), column.getName());
			System.out.print("\t|");
		}
		System.out.println();
		System.out.println("-----------------------------------------------------------------------------------------------------------------------------------------");
	}
	
	static void displayHeader(RandomAccessFile file, PageHeader pageHeader) throws IOException {
		file.seek(pageHeader.getPageStartFP());
		StringBuilder s = new StringBuilder();
		s.append(file.readByte());
		int num = file.readByte();
		s.append(" ").append(num);
		s.append(" ").append(file.readShort());
		s.append(" ").append(file.readInt());
		for (int i = 0; i < num; i++)
			s.append(" ").append(file.readShort());
		logMessage(s.toString());
	}
	
	static int getLastPk(Table table, boolean isCatalog) {
		try {
			RandomAccessFile tableFile;
			if(isCatalog) {
				//tableFile = new RandomAccessFile("Database/catalog/" + table.getTableName() + ".tbl", "rw");
				tableFile = new RandomAccessFile("data/catalog/" + table.getTableName() + ".tbl", "rw");
			}else
				tableFile = new RandomAccessFile(table.getFilePath(), "rw");
			int key = traverseAndGetLastPk(tableFile, 0);
			tableFile.close();
			return key;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	private static Column getColumn(RandomAccessFile file, long location, Table table) throws IOException {
		file.seek(location);
		HashMap<String, String> values = new HashMap<>();
		short payloadLength = file.readShort();
		if(payloadLength < 1)
			logMessage("Payload empty. Please check again.");
		Column pkColumn = table.getPkColumn();
		int pk = file.readInt();
		values.put(pkColumn.getName(), pk + "");
		byte numOfColumns = file.readByte();
		if(numOfColumns < 1)
			logMessage("Payload empty. Please check again.");
		for(Map.Entry<String, Column> entry : table.getColumns().entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			if(!column.isPk()) {
				byte typeByte = file.readByte();
				int numBytes = UtilityTools.getNumberOfBytesFromTypebyte(typeByte);
				byte[] byteArray = new byte[numBytes];
				file.read(byteArray);
				if(UtilityTools.valueNull(typeByte))
					values.put(column.getName(), "NULL");
				else
					values.put(column.getName(), column.getRecordValue(byteArray));
			}
		}
		return new Column(values.get("column_name"), values.get("data_type"), values.get("is_nullable").equals("YES"), Integer.parseInt(values.get("ordinal_position")), values.get("table_name"), values.get("database_name"), values.get("is_pk").equals("YES"));
	}

	static List<Column> traverseAndGetColumns(RandomAccessFile file, int pageNumber, Table table, String tableName, String dbName) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		if(pageHeader.getPageType() == 0x05) {
			//Traverse to the left most cell location.
			DataCellPage dataCellPage = new DataCellPage(file, pageHeader.getPageStartFP() + pageHeader.getCellLocations().get(0), false);
			return traverseAndGetColumns(file, dataCellPage.getLeftChildPointer(), table, tableName, dbName);
		} else if (pageHeader.getPageType() == 0x0D) {
			//Current node is table leaf.
			List<Short> cellLocations = pageHeader.getCellLocations();
			long pageStartPointer = pageHeader.getPageStartFP();
			if(pageHeader.getNumCells() == 0)
				return new ArrayList<>();
			List<Column> list = new ArrayList<>();
			Column tableNameColumn = table.getColumns().get("table_name");
			Column dbNameColumn = table.getColumns().get("database_name");
			int tableColumnIndex = table.getPkColumn().getOrdinalPosition() < tableNameColumn.getOrdinalPosition() ? tableNameColumn.getOrdinalPosition() - 1 : tableNameColumn.getOrdinalPosition();
			int dbNameColumnIndex = table.getPkColumn().getOrdinalPosition() < dbNameColumn.getOrdinalPosition() ? dbNameColumn.getOrdinalPosition() - 1 : dbNameColumn.getOrdinalPosition();
			int minIndex = Integer.min(tableColumnIndex, dbNameColumnIndex);
			int maxIndex = Integer.max(tableColumnIndex, dbNameColumnIndex);
			DataType tableValueTableName;
			DataType tableValueDbName;
			DataType minIndexDataType;
			DataType maxIndexDataType;
			for (Short cellLocation : cellLocations) {
				file.seek(pageStartPointer + cellLocation);
				file.skipBytes(6);
				int numColumns = file.readByte();
				if (tableColumnIndex >= numColumns || dbNameColumnIndex >= numColumns) {
					logMessage("Something very wrong happened to the database. Number of columns is less.");
					return list;
				}
				byte type;
				int i;
				for (i = 0; i < minIndex - 1; i++) {
					type = file.readByte();
					file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(type));
				}
				type = file.readByte();
				int bytesLength = UtilityTools.getNumberOfBytesFromTypebyte(type);
				byte[] bytes = new byte[bytesLength];
				file.read(bytes);
				minIndexDataType = getDataTypeFromByteType(type, bytes);

				for (i = minIndex; i < maxIndex - 1; i++) {
					type = file.readByte();
					file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(type));
				}

				type = file.readByte();
				bytesLength = UtilityTools.getNumberOfBytesFromTypebyte(type);
				bytes = new byte[bytesLength];
				file.read(bytes);
				maxIndexDataType = getDataTypeFromByteType(type, bytes);

				tableValueTableName = minIndex == tableColumnIndex ? minIndexDataType : maxIndexDataType;
				tableValueDbName = minIndex == dbNameColumnIndex ? minIndexDataType : maxIndexDataType;
				if (!tableValueTableName.equal(tableName) || !tableValueDbName.equal(dbName))
					continue;
				list.add(getColumn(file, pageStartPointer + cellLocation, table));
			}
			if(pageHeader.getRightChiSibPointer() != -1)
				list.addAll(traverseAndGetColumns(file, pageHeader.getRightChiSibPointer(), table, tableName, dbName));
			return list;
		} else {
			logMessage("Incorrect Page type");
			return new ArrayList<>();
		}
	}

	public static int binarySearchKey(RandomAccessFile file, PageHeader pageHeader, Condition condition) throws IOException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		int left = 0, right = pageHeader.getNumCells() - 1, mid;
		DataCellPage rightDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), true);
		DataCellPage leftDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(left), true);
		if(condition.value.greater(rightDataCellPage.getKey()))
			return -1;
		else if(condition.value.equal(rightDataCellPage.getKey()))
			return right;
		else if(condition.value.equal(leftDataCellPage.getKey()))
			return left;
		else if(condition.value.lesser(leftDataCellPage.getKey()))
			return -1;
		while(left <= right) {
			mid = (left + right) / 2;
			DataCellPage midDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), true);
			if(condition.value.greater(midDataCellPage.getKey()))
				left = mid + 1;
			else if(condition.value.lesser(midDataCellPage.getKey()))
				right = mid - 1;
			else
				return mid;
		}
		return -1;
	}

	static int largestKeyLesserEqual(RandomAccessFile file, PageHeader pageHeader, Condition condition, boolean isLeaf) throws IOException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		int left = 0, right = pageHeader.getNumCells() - 1, mid;
		DataCellPage leftDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(left), isLeaf);
		DataCellPage rightDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), isLeaf);
		if(condition.value.greaterEquals(rightDataCellPage.getKey()))
			return right;
		else if(condition.value.lesser(leftDataCellPage.getKey()))
			return -1;
		else if(condition.value.equal(leftDataCellPage.getKey()))
			return left;

		while(left != right) {
			mid = (left + right) / 2;
			DataCellPage midDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), isLeaf);
			if(condition.value.greater(midDataCellPage.getKey()))
				left = mid;
			else if(condition.value.lesser(midDataCellPage.getKey()))
				right = mid - 1;
			else
				return mid;
		}
		return left;
	}
	
	static int getHighestRightKeyValue(RandomAccessFile file, int pageNumber) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		if(pageHeader.getPageType() == 0) {
			logMessage("WARNING SOMETHING WENT WRONG BADLY");
			return 0;
		}
		if(pageHeader.getPageType() == 0x05) {
			return getHighestRightKeyValue(file, pageHeader.getRightChiSibPointer());
		} else if(pageHeader.getPageType() == 0x0D) {
			DataCellPage dcp = new DataCellPage(file, pageHeader.getPageStartFP() + pageHeader.getCellLocations().get(pageHeader.getNumCells() - 1), true);
			return dcp.getKey();
		} else {
			logMessage("Invalid page type. Get highest right value");
			return 0;
		}
	}

	static int smallestKeyGreaterEqual(RandomAccessFile file, PageHeader pageHeader, Condition condition, boolean isLeaf) throws IOException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		int left = 0, right = pageHeader.getNumCells() - 1, mid;
		DataCellPage leftDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(left), isLeaf);
		DataCellPage rightDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), isLeaf);
		if(condition.value.lesserEquals(leftDataCellPage.getKey()))
			return left;
		if(condition.value.greater(rightDataCellPage.getKey()))
			return -1;
		else if(condition.value.equal(rightDataCellPage.getKey()))
			return right;
		while(left != right) {
			mid = (left + right) / 2;
			DataCellPage midDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), isLeaf);
			if(condition.value.greater(midDataCellPage.getKey()))
				left = mid + 1;
			else if(condition.value.lesser(midDataCellPage.getKey()))
				right = mid;
			else
				return mid;
		}
		return left;
	}

	static int getLastPk(Table table) {
		return getLastPk(table, true);
	}

	private static int traverseAndGetLastPk(RandomAccessFile file, int pageNumber) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		if(pageHeader.getPageType() == 0x05) {
			if(pageHeader.getNumCells() == 0) {
				logMessage("Something is wrong");
				return 0;
			}
			return traverseAndGetLastPk(file, pageHeader.getRightChiSibPointer());
		} else if(pageHeader.getPageType() == 0x0D) {
			if(pageHeader.getNumCells() == 0) {
				logMessage("Empty page");
				return 0;
			}
			DataCellPage dataCellPage = new DataCellPage(file, pageHeader.getPageStartFP() + pageHeader.getCellLocations().get(pageHeader.getNumCells() - 1), true);
			return dataCellPage.getKey();
		} else {
			logMessage("Something is wrong");
			return 0;
		}
	}

	public static void wrongSyntax() {
		System.out.println("Wrong syntax please check again.");
	}
	
	static DataType getDataTypeFromByteType(byte type, byte[] bytes) {
		ByteBuffer bb = ByteBuffer.allocate(bytes.length);
		bb.put(bytes);
		switch (type) {
			case 0x04:
				return new TinyInt(bb.get(0));
			case 0x05:
				return new SmallInt(bb.getShort(0));
			case 0x06:
				return new IntType(bb.getInt(0));
			case 0x07:
				return new BigInt(bb.getLong(0));
			case 0x08:
				return new Real(bb.getFloat(0));
			case 0x09:
				return new DoubleType(bb.getDouble(0));
			case 0x0A:
				return new DateTimeType(bb.getLong(0));
			case 0x0B:
				return new DateType(bb.getLong(0));
			default:
				return new TextType(new String(bytes, StandardCharsets.US_ASCII));
		}
	}

}
