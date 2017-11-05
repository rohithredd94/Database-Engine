package com.marvelbase.Commands;

import com.marvelbase.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.marvelbase.MarvelBase.displayError;
import static com.marvelbase.MarvelBase.logMessage;

public class InsertCommand implements Command {

	private String command = null;
	private String currentDb;
	private InsertParams insertParams = null;

	public InsertCommand(String command, String currentDb) {
		this.command = command;
		this.currentDb = currentDb;
	}

	public InsertCommand(InsertParams insertParams) {
		this.insertParams = insertParams;
	}

	public SplitPage executeTraverseAndInsert() throws IOException, ParseException {
		return traverseAndInsert(insertParams.getTableFile(), insertParams.getPrimaryKey(), insertParams.getPageNumber(), insertParams.getValues(), insertParams.getTable(), insertParams.getRecord());
	}

	@Override
	public boolean execute() {
		if(command != null) {
			return parseInsertString();
		} else {
			displayError("Command not initialized");
			return false;
		}
	}
	
	private SplitPage traverseAndInsert(RandomAccessFile tableFile, int primaryKey, int pageNumber, LinkedHashMap<String, String> values, Table table, byte[] record) throws IOException, ParseException {
		PageHeader pageHeader = new PageHeader(tableFile, pageNumber);
		logMessage(pageHeader.getPageType() + " page type.");
		if(pageHeader.getPageType() == 0x05) {
			return traverseAndInsertInterior(tableFile, pageNumber, pageHeader, primaryKey, values, table, record);
		} else if (pageHeader.getPageType() == 0x0D) {
			return traverseAndInsertLeaf(tableFile, pageNumber, pageHeader, primaryKey, values, table, record);
		} else {
			logMessage("Incorrect Page type");
			return new SplitPage(false, 0);
		}
	}
	
	private void buildNewPageFromRecords(RandomAccessFile file, long pageStartFP, List<Record> pageRecords, int rightPointer, boolean isLeaf) throws IOException {
		file.seek(pageStartFP);
		file.writeByte(isLeaf ? 0x0D : 0x05);
		file.write(pageRecords.size());
		file.skipBytes(2);
		file.writeInt(rightPointer);
		byte[] bytes;
		short locationOffset = (short) UtilityTools.pageSize;
		int recordIndex = 0;
		for (Record record : pageRecords) {
			bytes = record.getBytes();
			locationOffset = (short) (locationOffset - bytes.length);
			file.seek(pageStartFP + 8 + 2 * recordIndex);
			file.writeShort(locationOffset);
			file.seek(pageStartFP + locationOffset);
			file.write(bytes);
			recordIndex++;
		}
		file.seek(pageStartFP + 2);
		file.writeShort(locationOffset);
	}
	
	private short addAtPosition(RandomAccessFile file, PageHeader pageHeader, int primaryKey) throws IOException {
		short left = 0, right = (short) (pageHeader.getNumCells() - 1), mid;
		long pageStartPointer = pageHeader.getPageStartFP();
		List<Short> cellLocations = pageHeader.getCellLocations();
		DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), true);
		if(primaryKey > dataCellPage.getKey())
			return (short) (right + 1);
		else if (primaryKey == dataCellPage.getKey())
			return -1;
		while(left != right) {
			mid = (short) ((left + right) / 2);
			dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), true);
			if(primaryKey > dataCellPage.getKey())
				left = (short) (mid + 1);
			else if (primaryKey < dataCellPage.getKey())
				right = mid;
			else
				return -1;
		}
		dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(left), true);
		logMessage(primaryKey + " : " + dataCellPage.getKey());
		if(primaryKey == dataCellPage.getKey())
			return -1;
		return left;
	}
	
	//insert into table (part_no, price) parts values (10, 120);
	private boolean parseInsertString() {
		logMessage("Parsing Insert command\n" + this.command);
		Pattern createTablePattern = Pattern.compile("insert into table (?:\\((.*)\\))? ([^( ]+) values \\((.+)\\)");
		Matcher commandMatcher = createTablePattern.matcher(this.command);
		if(commandMatcher.find()) {
			if(commandMatcher.group(2) == null || commandMatcher.group(3) == null) {
				CommandHelper.wrongSyntax();
				return false;
			}
			String dbTableName = commandMatcher.group(2).trim();
			String[] dbTableNameSplit = dbTableName.split("\\.");
			String tableName;
			String dbName;
			if(dbTableNameSplit.length > 1) {
				dbName = dbTableNameSplit[0];
				tableName = dbTableNameSplit[1];
			} else {
				if(this.currentDb == null) {
					displayError("No database selected");
					return false;
				}
				dbName = this.currentDb;
				tableName = dbTableName;
			}
			if(dbName.equalsIgnoreCase("catalog")) {
				displayError("You cannot insert records inside catalog.");
				return false;
			}
			File dbFile = new File("data/" + dbName);
			if(!dbFile.exists() || !dbFile.isDirectory()) {
				displayError("Database does not exist.");
				return false;
			}
			String fileName = "data/" + dbName + "/" + tableName + ".tbl";
			File f = new File(fileName);
			if (!f.exists()) {
				displayError("Table does not exist");
				return false;
			}
			List<Column> columns;
			try {
				RandomAccessFile sBColumnsTableFile = new RandomAccessFile("data/catalog/mb_columns.tbl", "rw");
				columns = CommandHelper.traverseAndGetColumns(sBColumnsTableFile, 0, UtilityTools.sbColumnsTable, tableName, dbName);
				columns.sort((column, t1) -> new Integer(column.getOrdinalPosition()).compareTo(t1.getOrdinalPosition()));
				sBColumnsTableFile.close();
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
			LinkedHashMap<String, Column> columnsHashMap = new LinkedHashMap<>();
			for (Column column : columns) {
				columnsHashMap.put(column.getName(), column);
			}
			Table table = new Table(dbName, tableName, columnsHashMap);
			LinkedHashMap<String, String> values = new LinkedHashMap<>();
			if(commandMatcher.group(1) != null) {
				String columnString = commandMatcher.group(1);
				String valuesString = commandMatcher.group(3);
				String[] columnStringSplit = columnString.trim().split(",");
				String[] valuesStringSplit = valuesString.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
				if (columnStringSplit.length != valuesStringSplit.length) {
					displayError("Incorrect number of columns and values specified");
					return false;
				}
				for (int i = 0; i < columnStringSplit.length; i++) {
					values.put(columnStringSplit[i].trim(), valuesStringSplit[i].trim().replaceAll("^\"|\"$", "").replaceAll("^'|'$", ""));
				}
				values.computeIfAbsent("row_id", k -> (CommandHelper.getLastPk(table, false) + 1) + "");
			} else {
				String valuesString = commandMatcher.group(3);
				String[] valuesStringSplit = valuesString.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
				if (columns.size() != valuesStringSplit.length) {
					displayError("Incorrect number of values specified");
					return false;
				}
				for (int i = 0; i < valuesStringSplit.length; i++) {
					values.put(columns.get(i).getName(), valuesStringSplit[i].trim().replaceAll("^\"|\"$", "").replaceAll("^'|'$", ""));
				}
			}
			if(table.validateValues(values)) {
				try {
					RandomAccessFile tableFile = new RandomAccessFile(table.getFilePath(), "rw");
					SplitPage splitPage = traverseAndInsert(tableFile, Integer.parseInt(values.get("row_id")), 0, values, table, null);
					tableFile.close();
					if(splitPage.isInserted()) {
						System.out.println("Inserted value");
					} else{
						return false;
					}
				} catch (IOException | ParseException e) {
					e.printStackTrace();
					return false;
				}
			} else {
				displayError("Incorrect values");
				return false;
			}
			return true;
		} else {
			CommandHelper.wrongSyntax();
			return false;
		}
	}

	private SplitPage traverseAndInsertInterior(RandomAccessFile tableFile, int pageNumber, PageHeader pageHeader, int primaryKey, LinkedHashMap<String, String> values, Table table, byte[] record) throws IOException, ParseException {
		short left = 0, right = (short) (pageHeader.getNumCells() - 1), mid;
		List<Short> cellLocations = pageHeader.getCellLocations();
		DataCellPage dataCellPage = new DataCellPage(tableFile, pageHeader.getPageStartFP() + cellLocations.get(right));
		if(primaryKey > dataCellPage.getKey()) {
			SplitPage splitPage = traverseAndInsert(tableFile, primaryKey, pageHeader.getRightChiSibPointer(), values, table, record);
			if(splitPage.isShouldSplit()) {
				//Check if space is available otherwise split.
				return checkAndSplitInteriorPage(tableFile, pageHeader, pageNumber, splitPage, (short) -1, 0);
			} else
				return splitPage;
		} else if(primaryKey == dataCellPage.getKey()) {
			if(values != null) {
				logMessage("Primary key must be unique.");
				return new SplitPage(false, -1);
			} else {
				SplitPage splitPage = traverseAndInsert(tableFile, primaryKey, dataCellPage.getLeftChildPointer(), null, table, record);
				if(splitPage.isShouldSplit()) {
					return checkAndSplitInteriorPage(tableFile, pageHeader, pageNumber, splitPage, (short) -1, 0);
				} else
					return splitPage;
			}
		}
		while(left != right) {
			mid = (short) ((left + right) / 2);
			dataCellPage = new DataCellPage(tableFile, pageHeader.getPageStartFP() + cellLocations.get(mid));
			if(primaryKey < dataCellPage.getKey())
				right = mid;
			else if(primaryKey > dataCellPage.getKey())
				left = (short) (mid + 1);
			else
				break;
		}
		if(left != right) {
			SplitPage splitPage = traverseAndInsert(tableFile, primaryKey, pageHeader.getRightChiSibPointer(), values, table, record);
			if(splitPage.isShouldSplit()) {
				dataCellPage = new DataCellPage(tableFile, pageHeader.getPageStartFP() + cellLocations.get(left));
				return checkAndSplitInteriorPage(tableFile, pageHeader, pageNumber, splitPage, left, dataCellPage.getLeftChildPointer());
			} else {
				return splitPage;
			}
		} else {
			if(values != null) {
				logMessage("Primary key must be unique.");
				return new SplitPage(false, -1);
			} else {
				SplitPage splitPage = traverseAndInsert(tableFile, primaryKey, dataCellPage.getLeftChildPointer(), null, table, record);
				if(splitPage.isShouldSplit()) {
					return checkAndSplitInteriorPage(tableFile, pageHeader, pageNumber, splitPage, (short) -1, 0);
				} else
					return splitPage;
			}
		}
	}

	private SplitPage traverseAndInsertLeaf(RandomAccessFile tableFile, int pageNumber, PageHeader pageHeader, int primaryKey, LinkedHashMap<String, String> values, Table table, byte[] record) throws IOException, ParseException {
		short space = (short) (pageHeader.getCellContentStartOffset() - pageHeader.getHeaderEndOffset());
		short recordLength = values != null ? table.getRecordLength(values) : (short) record.length;
		List<Short> cellLocations = pageHeader.getCellLocations();
		if(recordLength + 2 < space) {
			short locationOffset = (short) (pageHeader.getCellContentStartOffset() - recordLength);
			if(pageHeader.getNumCells() == 0) {
				tableFile.seek(pageHeader.getPageStartFP() + pageHeader.getHeaderEndOffset());
				tableFile.writeShort(locationOffset);
			} else {
				short addPosition = addAtPosition(tableFile, pageHeader, primaryKey);
				if(addPosition == -1) {
					logMessage("LEAF: Primary Key must be unique");
					return new SplitPage(false, -1);
				} else {
					long positionFP = pageHeader.getPageStartFP() + 8 + 2 * addPosition;
					tableFile.seek(positionFP);
					byte[] copyBytesFromPosition = null;
					if(pageHeader.getNumCells() != addPosition) {
						copyBytesFromPosition= new byte[2 * (pageHeader.getNumCells() - addPosition)];
						tableFile.read(copyBytesFromPosition);
						tableFile.seek(positionFP);
					}
					tableFile.writeShort(locationOffset);
					if(pageHeader.getNumCells() != addPosition && copyBytesFromPosition != null)
						tableFile.write(copyBytesFromPosition);
				}
			}
			tableFile.seek(pageHeader.getPageStartFP() + 1);
			tableFile.writeByte(pageHeader.getNumCells() + 1);
			tableFile.writeShort(locationOffset);
			//Update cell content start.
			tableFile.seek(pageHeader.getPageStartFP() + 2);
			tableFile.writeShort(locationOffset);
			int recordSize;
			if(values != null) {
				recordSize = table.writeRecord(tableFile, values, pageHeader.getPageStartFP() + locationOffset);
			} else {
				tableFile.seek(pageHeader.getPageStartFP() + locationOffset);
				tableFile.write(record);
				recordSize = record.length;
			}
			return new SplitPage(true, recordSize);
		} else {
			//Need to split page to make space.
			if(pageHeader.getNumCells() == 0) {
				logMessage("Wants to split a empty page!!");
				return new SplitPage(false, -1);
			}
			short addPosition = addAtPosition(tableFile, pageHeader, primaryKey);
			if(addPosition == -1) {
				logMessage("Primary key must be unique");
				return new SplitPage(false, -1);
			}
			int mid = pageHeader.getNumCells() / 2;
			List<Record> leftPage = new ArrayList<>();
			List<Record> rightPage = new ArrayList<>();
			//get the left page records
			int cellLocIndex = 0;
			Record recordObject = values != null ? table.getRecord(values) : new Record(record);
			for (int i = 0; i <= mid; i++) {
				if (i == addPosition) {
					leftPage.add(recordObject);
				} else {
					leftPage.add(new Record(tableFile, pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex), true));
					cellLocIndex++;
				}
				tableFile.seek(pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex - 1));
			}
			int midKey;
			if(addPosition == mid) {
				midKey = primaryKey;
			} else {
				DataCellPage midCell = new DataCellPage(tableFile, pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex - 1), true);
				midKey = midCell.getKey();
			}
			//get the right page records
			for (int i = mid + 1; i < pageHeader.getNumCells() + 1; i++) {
				if (i == addPosition) {
					rightPage.add(recordObject);
				} else {
					rightPage.add(new Record(tableFile, pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex), true));
					cellLocIndex++;
				}
			}
			long tableLength = tableFile.length();
			int rightPageNumber = (int) (tableLength / UtilityTools.pageSize);
			int leftPageNumber = (pageNumber == 0)? rightPageNumber + 1 : pageNumber;
			if(pageNumber != 0)
				tableFile.setLength(tableLength + UtilityTools.pageSize);
			else
				tableFile.setLength(tableLength + 2 * UtilityTools.pageSize);
			buildNewPageFromRecords(tableFile, leftPageNumber * UtilityTools.pageSize, leftPage, rightPageNumber, true);
			buildNewPageFromRecords(tableFile, rightPageNumber * UtilityTools.pageSize, rightPage, pageHeader.getRightChiSibPointer(), true);
			if(addPosition == mid)
				midKey = primaryKey;
			if(pageNumber == 0) {
				tableFile.seek(pageHeader.getPageStartFP());
				tableFile.writeByte(0x05);
				tableFile.writeByte(1);
				tableFile.writeShort((int) (UtilityTools.pageSize - 8));
				tableFile.writeInt(rightPageNumber);
				tableFile.writeShort((int) (UtilityTools.pageSize - 8));
				tableFile.seek(pageHeader.getPageStartFP() + (UtilityTools.pageSize - 8));
				tableFile.writeInt(leftPageNumber);
				tableFile.writeInt(midKey);
				return new SplitPage(true, recordObject.getRecordLength());
			} else {
				return new SplitPage(midKey, rightPageNumber, recordObject.getRecordLength());
			}
		}
	}

	private SplitPage checkAndSplitInteriorPage(RandomAccessFile tableFile, PageHeader pageHeader, int pageNumber, SplitPage splitPage, short position, int oldLeftPointer) throws IOException {
		short space = (short) (pageHeader.getCellContentStartOffset() - pageHeader.getHeaderEndOffset());
		short recordLength = 8;
		if(recordLength + 2 < space) {
			//Space is available for the new key.
			short locationOffset = (short) (pageHeader.getCellContentStartOffset() - recordLength);
			tableFile.seek(pageHeader.getPageStartFP() + locationOffset);
			if(position == -1) {
				//Write the new key and its left pointer which should be the right pointer of the page
				tableFile.writeInt(pageHeader.getRightChiSibPointer());
				tableFile.writeInt(splitPage.getKey());
				//Update the new right pointer of the page to the new page created.
				tableFile.seek(pageHeader.getPageStartFP() + 4);
				tableFile.writeInt(splitPage.getPageNumber());
				tableFile.seek(pageHeader.getPageStartFP() + pageHeader.getHeaderEndOffset());
				tableFile.writeShort(locationOffset);
			} else {

				tableFile.writeInt(oldLeftPointer);
				tableFile.writeInt(splitPage.getKey());
				tableFile.seek(pageHeader.getPageStartFP() + 8 + 2 * position);
				short cellLocation = tableFile.readShort();
				tableFile.seek(pageHeader.getPageStartFP()+ cellLocation);
				tableFile.writeInt(splitPage.getPageNumber());

				tableFile.seek(pageHeader.getPageStartFP() + 8 + 2 * position);
				short bytesLength = (short) (pageHeader.getHeaderEndOffset() - 8 - 2 * position);
				byte[] bytes = new byte[bytesLength];
				tableFile.read(bytes, 0, bytesLength);
				tableFile.seek(pageHeader.getPageStartFP() + 8 + 2 * position);
				tableFile.writeShort(locationOffset);
				tableFile.write(bytes);
			}
			tableFile.seek(pageHeader.getPageStartFP() + 1);
			tableFile.writeByte(pageHeader.getNumCells() + 1);
			tableFile.writeShort(locationOffset);
			return new SplitPage(true, splitPage.getInsertedSize());
		} else {
			//Need to split to make space.
			List<Short> cellLocations = pageHeader.getCellLocations();
			//If position is not last, update the present position's left child pointer to the new page pointer.
			if(position != -1) {
				tableFile.seek(pageHeader.getPageStartFP() + 8 + 2 * position);
				short cellLocation = tableFile.readShort();
				tableFile.seek(pageHeader.getPageStartFP()+ cellLocation);
				tableFile.writeInt(splitPage.getPageNumber());
			} else {
				oldLeftPointer = pageHeader.getRightChiSibPointer();
			}

			//Get the mid position of the locations.
			int mid = pageHeader.getNumCells() / 2;
			List<Record> leftPage = new ArrayList<>();
			List<Record> rightPage = new ArrayList<>();
			InteriorCell newInteriorCell = new InteriorCell(oldLeftPointer, splitPage.getKey());
			//get the left page records
			int cellLocIndex = 0;
			for (int i = 0; i < mid; i++) {
				if (i == position) {
					leftPage.add(newInteriorCell.getRecord());
				} else {
					leftPage.add(new Record(tableFile, pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex), false));
					cellLocIndex++;
				}
			}

			int midKey;
			int midLeftPointer;
			if(position == mid) {
				midKey = newInteriorCell.getKey();
				midLeftPointer = newInteriorCell.getLeftChildPointer();
			} else {
				DataCellPage midCell = new DataCellPage(tableFile, pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex));
				midKey = midCell.getKey();
				midLeftPointer = midCell.getLeftChildPointer();
				cellLocIndex++;
			}

			//get the right page records
			for (int i = mid + 1; i < pageHeader.getNumCells() + 1; i++) {
				if (i == position) {
					rightPage.add(newInteriorCell.getRecord());
				} else if(cellLocIndex < pageHeader.getNumCells()) {
					rightPage.add(new Record(tableFile, pageHeader.getPageStartFP() + cellLocations.get(cellLocIndex), false));
					cellLocIndex++;
				}
			}
			//If position is last, add to right page.
			if(position == -1)
				rightPage.add(newInteriorCell.getRecord());

			long tableLength = tableFile.length();
			
			int rightPageNumber = (int) (tableLength / UtilityTools.pageSize);
			
			int leftPageNumber = (pageNumber == 0)? rightPageNumber + 1 : pageNumber;
			
			if(pageNumber != 0)
				tableFile.setLength(tableLength + UtilityTools.pageSize);
			else
				tableFile.setLength(tableLength + 2 * UtilityTools.pageSize);

			//Start building the pages.
			//Left page's right pointer must be the mid's left pointer.
			buildNewPageFromRecords(tableFile, leftPageNumber * UtilityTools.pageSize, leftPage, midLeftPointer, false);
			//Right page's right pointer will be the new child pointer if the key is added at last, otherwise it will be the old page's right pointer.
			buildNewPageFromRecords(tableFile, rightPageNumber * UtilityTools.pageSize, rightPage, position == -1 ? splitPage.getPageNumber() : pageHeader.getRightChiSibPointer(), false);

			//Check if page is root.
			if(pageNumber == 0) {
				//The page is root. So update it
				tableFile.seek(pageHeader.getPageStartFP());
				tableFile.writeByte(0x05);
				tableFile.writeByte(1);
				tableFile.writeShort((int) (UtilityTools.pageSize - 8));
				//Right child pointer is the new right page number.
				tableFile.writeInt(rightPageNumber);
				tableFile.writeShort((int) (UtilityTools.pageSize - 8));
				tableFile.seek(pageHeader.getPageStartFP() + (UtilityTools.pageSize - 8));
				tableFile.writeInt(leftPageNumber);
				tableFile.writeInt(midKey);
				return new SplitPage(true, splitPage.getInsertedSize());
			} else {
				return new SplitPage(midKey, rightPageNumber, splitPage.getInsertedSize());
			}
		}
	}

}
