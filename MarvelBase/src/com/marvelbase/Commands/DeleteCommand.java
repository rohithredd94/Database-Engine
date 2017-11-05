package com.marvelbase.Commands;

import com.marvelbase.*;
import com.marvelbase.DataType.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.marvelbase.MarvelBase.displayError;
import static com.marvelbase.MarvelBase.logMessage;
import static com.marvelbase.MarvelBase.response;

public class DeleteCommand implements Command {

	private String command = null;
	private String currentDb;
	private DeleteParams deleteParams = null;

	public DeleteCommand(String command, String currentDb) {
		this.command = command;
		this.currentDb = currentDb;
	}

	public DeleteCommand(DeleteParams deleteParams) {
		this.deleteParams = deleteParams;
	}

	public DeleteCommand() {
	}

	@Override
	public boolean execute() {
		if(command == null) {
			displayError("FATAL ERROR: Command not initialized");
			return false;
		}
		return parseDeleteString();
	}

	//Setting delete params and executing the command.
	DeleteResult executeTraverseAndDelete() throws IOException {
		if(deleteParams == null)
			return null;
		return traverseAndDelete(deleteParams.getFile(), deleteParams.getPageNumber(), deleteParams.getTable(), deleteParams.getCondition());
	}

	private boolean parseDeleteString() {
		Pattern pattern = Pattern.compile("^delete from table (\\S+)(?: where (.+)$|$)");
		Matcher matcher = pattern.matcher(this.command);
		if(matcher.find()) {
			if(matcher.groupCount() == 2) {
				System.out.println(matcher.group(1) + " "+ matcher.group(2));
				return checkAndExecuteDeleteString(matcher.group(1), matcher.group(2));
			} else {
				CommandHelper.wrongSyntax();
				return false;
			}
		} else {
			CommandHelper.wrongSyntax();
			return false;
		}
	}
	
	private void deleteCell(RandomAccessFile file, int key, PageHeader pageHeader) throws IOException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartFP = pageHeader.getPageStartFP();
		short searchKeyCellLocation = cellLocations.get(key);
		logMessage("DELETING cell in leaf");
		file.seek(pageStartFP + searchKeyCellLocation);
		short numOfBytes = 8;
		file.seek(pageStartFP + pageHeader.getCellContentStartOffset());
		int copyBytesLength = pageHeader.getCellContentStartOffset() - searchKeyCellLocation;
		byte[] copyBytes = new byte[copyBytesLength];
		file.read(copyBytes);
		file.seek(pageStartFP + pageHeader.getCellContentStartOffset() + numOfBytes);
		file.write(copyBytes);
		//Update cell content start
		file.seek(pageStartFP + 2);
		file.writeShort(pageHeader.getCellContentStartOffset() + numOfBytes);
		//Update cell locations
		int index = 0;
		for (Short cellLocation : cellLocations) {
			if(cellLocation < searchKeyCellLocation) {
				file.seek(pageStartFP + 8 + 2 * index);
				file.writeShort(cellLocation + numOfBytes);
			}
			index++;
		}
		//Remove the key cell locations in the header
		file.seek(pageStartFP + 8 + 2 * (key + 1));
		copyBytesLength = pageHeader.getHeaderEndOffset() - (8 + 2 * (key + 1));
		copyBytes = new byte[copyBytesLength];
		file.read(copyBytes);
		file.seek(pageStartFP + 8 + 2 * key);
		file.write(copyBytes);
		//Update the number of cells
		file.seek(pageStartFP + 1);
		file.writeByte(pageHeader.getNumCells() - 1);
	}
	
	private DeleteResult traverseAndDeleteCatalogLeaf(RandomAccessFile file, int pageNumber, PageHeader pageHeader, Table table, String tableName, String dbName) throws IOException {
		DeleteResult deleteResult = new DeleteResult();
		deleteResult.setLeaf(true);
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

		long pageStartPointer = pageHeader.getPageStartFP();
		List<Short> cellLocations = pageHeader.getCellLocations();
		int sizeOfCellLocations = cellLocations.size();
		for (int j = 0; j < sizeOfCellLocations; j++) {
			file.seek(pageStartPointer + cellLocations.get(j - deleteResult.getNumOfRecordsDeleted()));
			file.skipBytes(6);
			int numColumns = file.readByte();
			if (tableColumnIndex >= numColumns || dbNameColumnIndex >= numColumns) {
				logMessage("Something very wrong happened to the database. Number of columns is less.");
				return deleteResult;
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
			minIndexDataType = CommandHelper.getDataTypeFromByteType(type, bytes);

			for (i = minIndex; i < maxIndex - 1; i++) {
				type = file.readByte();
				file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(type));
			}

			type = file.readByte();
			bytesLength = UtilityTools.getNumberOfBytesFromTypebyte(type);
			bytes = new byte[bytesLength];
			file.read(bytes);
			maxIndexDataType = CommandHelper.getDataTypeFromByteType(type, bytes);

			tableValueTableName = minIndex == tableColumnIndex ? minIndexDataType : maxIndexDataType;
			tableValueDbName = minIndex == dbNameColumnIndex ? minIndexDataType : maxIndexDataType;
			if (!tableValueTableName.equal(tableName) || !tableValueDbName.equal(dbName))
				continue;
			int deletedPk = deleteRecord(file, j - deleteResult.getNumOfRecordsDeleted(), pageHeader);
			deleteResult.deleteKey(deletedPk);
			pageHeader = new PageHeader(file, pageNumber);
			cellLocations = pageHeader.getCellLocations();
			if(pageHeader.getNumCells() == 0 && pageNumber != 0) {
				deleteResult.setWholePageDeleted(true);
				deleteResult.setRightSiblingPageNumber(pageHeader.getRightChiSibPointer());
				deleteResult.setUpdateRightMostChildRightPointer(true);
				file.seek(pageStartPointer);
				file.write(0x00);
			}
		}
		return deleteResult;
	}

	private DeleteResult updateDeleteAfterTraversal(RandomAccessFile file, int key, PageHeader pageHeader, int pageNumber , DeleteResult deleteResult, DeleteResult subDeleteResult, int dcpKey) throws IOException {

		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		//If we need to update the right most child pointer and there are keys before, update else send to top. If page is root then dont do anything if there are no keys.
		if(subDeleteResult.isUpdateRightMostChildRightPointer()) {
			if((key == 0 || (key == -1 && pageHeader.getNumCells() == 0)) && pageNumber != 0) {
				deleteResult.setUpdateRightMostChildRightPointer(true);
				deleteResult.setRightSiblingPageNumber(subDeleteResult.getRightSiblingPageNumber());
			} else if(!(key == 0 || (key == -1 && pageHeader.getNumCells() == 0))) {
				DataCellPage dataCellPage1 = new DataCellPage(file, pageStartPointer + cellLocations.get(key == -1 ? cellLocations.size() - 1 : key - 1), false);
				updateRightMostChildRightPointer(file, dataCellPage1.getLeftChildPointer(), subDeleteResult.getRightSiblingPageNumber());
			}
		}

		if(subDeleteResult.getOnePageNumber()!= -1) {
			if(key != -1) {
				file.seek(pageStartPointer + cellLocations.get(key));
				file.writeInt(subDeleteResult.getOnePageNumber());
			} else {
				file.seek(pageStartPointer + 4);
				file.writeInt(subDeleteResult.getOnePageNumber());
			}
		}

		if(subDeleteResult.isWholePageDeleted()) {
			if(key != -1) {
				deleteCell(file, key, pageHeader);
			} else {
				pageHeader = new PageHeader(file, pageNumber);
				if (pageHeader.getNumCells() == 0 && pageNumber != 0) {
					deleteResult.setWholePageDeleted(true);
					file.seek(pageStartPointer);
					file.writeByte(0x00);
				} else if(pageHeader.getNumCells() == 0) {
					buildBlankPage(file, 0);
					deleteResult.setWholePageDeleted(true);
					return deleteResult;
				} else if(pageHeader.getNumCells() == 1 && pageNumber != 0) {
					file.seek(pageStartPointer + cellLocations.get(0));
					deleteResult.setOnePageNumber(file.readInt());
					file.seek(pageStartPointer);
					file.writeByte(0x00);
				} else if(pageHeader.getNumCells() == 1) {
					file.seek(pageStartPointer + cellLocations.get(0));
					int onePageNumber = file.readInt();
					file.seek(onePageNumber * UtilityTools.pageSize);
					byte[] bytes = new byte[(int) UtilityTools.pageSize];
					file.seek(onePageNumber * UtilityTools.pageSize);
					file.writeByte(0x00);
					file.seek(pageStartPointer);
					file.write(bytes);
				} else {
					file.seek(pageStartPointer + cellLocations.get(cellLocations.size() - 1));
					int newRightPointer = file.readInt();
					file.seek(pageStartPointer + 4);
					file.writeInt(newRightPointer);
					pageHeader = new PageHeader(file, pageNumber);
					deleteCell(file, cellLocations.size() - 1, pageHeader);
				}
			}
		} else if(key != -1) {
			//Update the cell last record key.
			if (subDeleteResult.keyIsDeleted(dcpKey)) {
				DataCellPage dcp = new DataCellPage(file, pageStartPointer + cellLocations.get(key), false);
				int getHighestRightValue = CommandHelper.getHighestRightKeyValue(file, dcp.getLeftChildPointer());
				//TODO: something is wrong. It is not copying the correct key.
				file.seek(pageStartPointer + cellLocations.get(key) + 4);
				//file.seek(pageStartPointer + cellLocations.get(key));
				file.writeInt(getHighestRightValue);
			}
		} else {
			if(pageHeader.getNumCells() == 0 && pageNumber != 0) {
				file.seek(pageStartPointer + 4);
				deleteResult.setOnePageNumber(file.readInt());
				file.seek(pageStartPointer);
				file.writeByte(0x00);
			} else if(pageHeader.getNumCells() == 0) {
				file.seek(pageStartPointer + 4);
				int onePageNumber = file.readInt();
				file.seek(onePageNumber * UtilityTools.pageSize);
				byte[] bytes = new byte[(int) UtilityTools.pageSize];
				file.seek(onePageNumber * UtilityTools.pageSize);
				file.writeByte(0x00);
				file.seek(pageStartPointer);
				file.write(bytes);
			}
		}
		deleteResult.mergeSubResult(subDeleteResult);
		return deleteResult;
	}

	private DeleteResult traverseAndDelete(RandomAccessFile file, int pageNumber, Table table, Condition condition) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		DeleteResult deleteResult = new DeleteResult();
		if(condition != null && condition.column.isPk() && condition.operation.equals("is null"))
			return deleteResult;
		if(condition != null && condition.column.isPk() && condition.operation.equals("is not null"))
			condition = null;
		if(pageHeader.getNumCells() == 0)
			return deleteResult;
		if(pageHeader.getPageType() == 0x05) {
			//Current page is table interior
			return traverseAndDeleteInterior(file, pageNumber, pageHeader, table, condition);
		} else if(pageHeader.getPageType() == 0x0D) {
			//Current page is table leaf
			return traverseAndDeleteLeaf(file, pageNumber, pageHeader, table, condition);
		} else {
			logMessage("Invalid Page");
			return deleteResult;
		}
	}

	private DeleteResult traverseAndDeleteInterior(RandomAccessFile file, int pageNumber, PageHeader pageHeader, Table table, Condition condition) throws IOException {
		DeleteResult deleteResult = new DeleteResult();
		deleteResult.setLeaf(false);
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		if(condition != null && condition.column.isPk() && (condition.operation.equals(">") || condition.operation.equals(">=") || condition.operation.equals("=") || condition.operation.equals("<") || condition.operation.equals("<="))) {
			if(condition.operation.equals("=")) {
				int key = CommandHelper.smallestKeyGreaterEqual(file, pageHeader, condition, false);
				int traversePointer = -1;
				DataCellPage dataCellPage = null;
				if(key == -1)
					traversePointer = pageHeader.getRightChiSibPointer();
				else
					dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), false);
				DeleteResult subDeleteResult = traverseAndDelete(file, key == -1 ? traversePointer : dataCellPage.getLeftChildPointer(), table, condition);
				deleteResult = updateDeleteAfterTraversal(file, key, pageHeader, pageNumber, deleteResult, subDeleteResult, key == -1 ? -1: dataCellPage.getKey());
				return deleteResult;
			} else if(condition.operation.equals("<") || condition.operation.equals("<=")) {
				int numCellsDeleted = 0;
				int key = CommandHelper.largestKeyLesserEqual(file, pageHeader, condition, false);
				if(key == -1) {
					return deleteResult;
				}
				if(condition.operation.equals("<")) {
					DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), false);
					if(condition.value.equal(dataCellPage.getKey()))
						key--;
				}
				for (int i = 0; i <= key; i++) {
					//Delete Record and update page header.
					DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(i - numCellsDeleted), false);
					DeleteResult subDeleteResult = traverseAndDelete(file, dataCellPage.getLeftChildPointer(), table, condition);
					deleteResult = updateDeleteAfterTraversal(file, i - numCellsDeleted, pageHeader, pageNumber, deleteResult, subDeleteResult, dataCellPage.getKey());
					if(subDeleteResult.isWholePageDeleted())
						numCellsDeleted++;
					pageHeader = new PageHeader(file, pageNumber);
					cellLocations = pageHeader.getCellLocations();
				}
				return deleteResult;
			} else {
				int numCellsDeleted = 0;
				int key = CommandHelper.smallestKeyGreaterEqual(file, pageHeader, condition, false);
				if(key == -1) {
					logMessage("Something went wrong. skGE is -1. Should not be in this page.");
					return new DeleteResult();
				}
				if(condition.operation.equals(">")) {
					DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), true);
					if(condition.value.equal(dataCellPage.getKey()))
						key++;
				}
				int cellLocationsSize = cellLocations.size();
				int i;
				for (i = key; i < cellLocationsSize; i++) {
					//Delete Record and update page header.
					DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(i - numCellsDeleted), false);
					DeleteResult subDeleteResult = traverseAndDelete(file, dataCellPage.getLeftChildPointer(), table, condition);
					deleteResult = updateDeleteAfterTraversal(file, i - numCellsDeleted, pageHeader, pageNumber, deleteResult, subDeleteResult, dataCellPage.getKey());
					if(subDeleteResult.isWholePageDeleted())
						numCellsDeleted++;
					pageHeader = new PageHeader(file, pageNumber);
					cellLocations = pageHeader.getCellLocations();
				}
				if(i == cellLocationsSize) {
					DeleteResult subDeleteResult = traverseAndDelete(file, pageHeader.getRightChiSibPointer(), table, condition);
					deleteResult = updateDeleteAfterTraversal(file, -1, pageHeader, pageNumber, deleteResult, subDeleteResult, -1);
				}
				return deleteResult;
			}
		} else {
			int numCellsDeleted = 0;
			int cellLocationsSize = cellLocations.size();
			if(cellLocationsSize == 0) {
				logMessage("Something went wrong. no cells in this page.");
				return new DeleteResult();
			}
			for (int i = 0; i < cellLocationsSize; i++) {
				//Delete Record and update page header.
				DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(i - numCellsDeleted), false);
				DeleteResult subDeleteResult = traverseAndDelete(file, dataCellPage.getLeftChildPointer(), table, condition);
				deleteResult = updateDeleteAfterTraversal(file, i - numCellsDeleted, pageHeader, pageNumber, deleteResult, subDeleteResult, dataCellPage.getKey());
				if(subDeleteResult.isWholePageDeleted())
					numCellsDeleted++;
				pageHeader = new PageHeader(file, pageNumber);
				cellLocations = pageHeader.getCellLocations();
			}
			DeleteResult subDeleteResult = traverseAndDelete(file, pageHeader.getRightChiSibPointer(), table, condition);
			deleteResult = updateDeleteAfterTraversal(file, -1, pageHeader, pageNumber, deleteResult, subDeleteResult, -1);
			return deleteResult;
		}
	}

	private DeleteResult traverseAndDeleteLeaf(RandomAccessFile file, int pageNumber, PageHeader pageHeader, Table table, Condition condition) throws IOException {
		DeleteResult deleteResult = new DeleteResult();
		deleteResult.setLeaf(true);
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		if(condition != null && condition.column.isPk() && (condition.operation.equals(">") || condition.operation.equals(">=") || condition.operation.equals("=") || condition.operation.equals("<") || condition.operation.equals("<="))) {
			if(condition.operation.equals("=")) {
				int searchKey = CommandHelper.binarySearchKey(file, pageHeader, condition);
				if (searchKey == -1)
					return deleteResult;
				//Delete Record and update page header.
				int deletedPk = deleteRecord(file, searchKey, pageHeader);
				deleteResult.deleteKey(deletedPk);
				pageHeader = new PageHeader(file, pageNumber);
				if(pageHeader.getNumCells() == 0 && pageNumber != 0) {
					deleteResult.setWholePageDeleted(true);
					deleteResult.setRightSiblingPageNumber(pageHeader.getRightChiSibPointer());
					file.seek(pageStartPointer);
					file.write(0x00);
				}
				return deleteResult;
			} else if(condition.operation.equals("<") || condition.operation.equals("<=")) {
				int key = CommandHelper.largestKeyLesserEqual(file, pageHeader, condition, true);
				if(key == -1) {
					return deleteResult;
				}
				if(condition.operation.equals("<")) {
					DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), true);
					if(condition.value.equal(dataCellPage.getKey()))
						key--;
				}
				for (int i = 0; i <= key; i++) {
					//Delete Record and update page header.
					int deletedPk = deleteRecord(file, i - deleteResult.getNumOfRecordsDeleted(), pageHeader);
					deleteResult.deleteKey(deletedPk);
					pageHeader = new PageHeader(file, pageNumber);
				}
				if(pageHeader.getNumCells() == 0 && pageNumber != 0) {
					deleteResult.setWholePageDeleted(true);
					deleteResult.setRightSiblingPageNumber(pageHeader.getRightChiSibPointer());
					deleteResult.setUpdateRightMostChildRightPointer(true);
					file.seek(pageStartPointer);
					file.write(0x00);
				}
				return deleteResult;
			} else {
				int key = CommandHelper.smallestKeyGreaterEqual(file, pageHeader, condition, true);
				if(key == -1) {
					logMessage("Something went wrong. skGE is -1. Should not be in this page.");
					return new DeleteResult();
				}
				if(condition.operation.equals(">")) {
					DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), true);
					if(condition.value.equal(dataCellPage.getKey()))
						key++;
				}
				for (int i = key; i < cellLocations.size(); i++) {
					int deletedPk = deleteRecord(file, i - deleteResult.getNumOfRecordsDeleted(), pageHeader);
					deleteResult.deleteKey(deletedPk);
					pageHeader = new PageHeader(file, pageNumber);
				}
				if(pageHeader.getNumCells() == 0 && pageNumber != 0) {
					deleteResult.setWholePageDeleted(true);
					deleteResult.setRightSiblingPageNumber(pageHeader.getRightChiSibPointer());
					deleteResult.setUpdateRightMostChildRightPointer(true);
					file.seek(pageStartPointer);
					file.write(0x00);
				}
				return deleteResult;
			}
		} else {
			int columnIndex = -1;
			if(condition != null && !condition.column.isPk()) {
				columnIndex = table.getPkColumn().getOrdinalPosition() < condition.column.getOrdinalPosition() ? condition.column.getOrdinalPosition() - 1 : condition.column.getOrdinalPosition();
			}
			boolean conditionColumnIsPk = condition != null && condition.column.isPk();
			int sizeOfCellLocations = cellLocations.size();
			for (int j = 0; j < sizeOfCellLocations; j++) {
				file.seek(pageStartPointer + cellLocations.get(j - deleteResult.getNumOfRecordsDeleted()));
				if(condition != null) {
					if(conditionColumnIsPk) {
						file.skipBytes(2);
						int pk = file.readInt();
						if(!condition.result(new IntType(pk)))
							continue;
					} else {
						file.skipBytes(6);
						int numColumns = file.readByte();
						if (columnIndex >= numColumns) {
							logMessage("Something very wrong happened to the database. Number of columns is less.");
							return deleteResult;
						}
						byte type;
						for (int i = 0; i < columnIndex - 1; i++) {
							type = file.readByte();
							file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(type));
						}
						type = file.readByte();
						switch (condition.operation) {
							case "is null":
								if (!UtilityTools.valueNull(type) && type != 0x0C)
									continue;
								break;
							case "is not null":
								if (UtilityTools.valueNull(type) || type == 0x0C)
									continue;
								break;
							default:
								if (UtilityTools.valueNull(type)) {
									continue;
								} else {
									int bytesLength = UtilityTools.getNumberOfBytesFromTypebyte(type);
									byte[] bytes = new byte[bytesLength];
									file.read(bytes);
									DataType tableValue = CommandHelper.getDataTypeFromByteType(type, bytes);
									if (!condition.result(tableValue))
										continue;
								}
								break;
						}
					}
				}
				int deletedPk = deleteRecord(file, j - deleteResult.getNumOfRecordsDeleted(), pageHeader);
				deleteResult.deleteKey(deletedPk);
				pageHeader = new PageHeader(file, pageNumber);
				cellLocations = pageHeader.getCellLocations();
			}
			if(pageHeader.getNumCells() == 0 && pageNumber != 0) {
				deleteResult.setWholePageDeleted(true);
				deleteResult.setRightSiblingPageNumber(pageHeader.getRightChiSibPointer());
				deleteResult.setUpdateRightMostChildRightPointer(true);
				file.seek(pageStartPointer);
				file.write(0x00);
			}
			return deleteResult;
		}
	}
	
	private boolean checkAndExecuteDeleteString(String dbTableName, String condition) {
		dbTableName = dbTableName.trim();
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
			dbName = currentDb;
			tableName = dbTableName;
		}
		if(dbName.equalsIgnoreCase("catalog")) {
			displayError("You cannot delete records inside catalog.");
			return false;
		}
		//File dbFile = new File("Database/" + dbName);
		File dbFile = new File("data/" + dbName);
		if(!dbFile.exists() || !dbFile.isDirectory()) {
			displayError("Database does not exist.");
			return false;
		}
		//String fileName = "Database/" + dbName + "/" + tableName + ".tbl";
		String fileName = "data/" + dbName + "/" + tableName + ".tbl";
		File f = new File(fileName);
		if (!f.exists()) {
			displayError("Table does not exist.");
			return false;
		}

		try {
			//RandomAccessFile sBColumnsTableFile = new RandomAccessFile("Database/catalog/sb_columns.tbl", "rw");
			//RandomAccessFile sBColumnsTableFile = new RandomAccessFile("data/catalog/sb_columns.tbl", "rw");
			RandomAccessFile sBColumnsTableFile = new RandomAccessFile("data/catalog/mb_columns.tbl", "rw");
			List<Column> columns = CommandHelper.traverseAndGetColumns(sBColumnsTableFile, 0, UtilityTools.sbColumnsTable, tableName, dbName);
			sBColumnsTableFile.close();
			columns.sort((column, t1) -> new Integer(column.getOrdinalPosition()).compareTo(t1.getOrdinalPosition()));
			LinkedHashMap<String, Column> columnHashMap = new LinkedHashMap<>();
			for (Column column: columns) {
				columnHashMap.put(column.getName(), column);
			}
			Table table = new Table(dbName, tableName, columnHashMap);
			RandomAccessFile tableFile = new RandomAccessFile(table.getFilePath(), "rw");
			Condition conditionObj = null;
			if(condition != null) {
				Pattern pattern = Pattern.compile("^(\\S+) (= |!= |> |>= |< |<= |like |is null$|is not null$)(.*)");
				Matcher matcher = pattern.matcher(condition);
				if(matcher.find()) {
					if (matcher.groupCount() == 3) {
						Column column = columnHashMap.get(matcher.group(1));
						if(column == null) {
							displayError("Condition Column not found");
							return false;
						}
						String operationString = matcher.group(2).trim();
						if(matcher.group(3) != null && !matcher.group(3).equals("")) {
							if(operationString.equals("is null") || operationString.equals("is not null")) {
								CommandHelper.wrongSyntax();
								return false;
							}
							DataType columnValue = column.getColumnValue(matcher.group(3));
							if(columnValue instanceof TextType || columnValue instanceof DateTimeType || columnValue instanceof DateType) {
								if(UtilityTools.regexSatisfy((String) columnValue.getValue(), "^\".*\"$"))
									columnValue = new TextType(((String) columnValue.getValue()).replaceAll("^\"|\"$", ""));
								else if(UtilityTools.regexSatisfy((String) columnValue.getValue(), "^'.*'$"))
									columnValue = new TextType(((String) columnValue.getValue()).replaceAll("^'|'$", ""));
								else {
									displayError("String, dates, datetime columns must be string and must be in '' or \"\"");
									return false;
								}
							}
							conditionObj = new Condition(column, operationString, columnValue);
						} else {
							if(!operationString.equals("is null") && !operationString.equals("is not null")) {
								CommandHelper.wrongSyntax();
								return false;
							}
							conditionObj = new Condition(column, operationString, null);
						}
					} else {
						CommandHelper.wrongSyntax();
						return false;
					}
				}
			}
			DeleteResult deleteResult = traverseAndDelete(tableFile, 0, table, conditionObj);
			response(deleteResult.getNumOfRecordsDeleted() + "");
			return true;
		} catch (IOException | ParseException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	private void buildBlankPage(RandomAccessFile file, int pageNumber) throws IOException {
		long pageStart = pageNumber * UtilityTools.pageSize;
		file.seek(pageStart);
		file.writeByte(0x0D);
		file.writeByte(0x00);
		file.writeShort((int) UtilityTools.pageSize);
		file.writeInt(-1);
	}

	private int deleteRecord(RandomAccessFile file, int searchKey, PageHeader pageHeader) throws IOException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartFP = pageHeader.getPageStartFP();
		short searchKeyCellLocation = cellLocations.get(searchKey);
		file.seek(pageStartFP + searchKeyCellLocation);
		short numOfBytes = (short) (file.readShort() + 6);
		int pk = file.readInt();
		logMessage("Deleting " + pk + " in page " + (pageStartFP / UtilityTools.pageSize));
		//Push the content to last.
		file.seek(pageStartFP + pageHeader.getCellContentStartOffset());
		int copyBytesLength = searchKeyCellLocation - pageHeader.getCellContentStartOffset();
		byte[] copyBytes = new byte[copyBytesLength];
		file.read(copyBytes);
		file.seek(pageStartFP + pageHeader.getCellContentStartOffset() + numOfBytes);
		file.write(copyBytes);
		//Update cell locations
		int index = 0;
		for (Short cellLocation : cellLocations) {
			if(cellLocation < searchKeyCellLocation) {
				file.seek(pageStartFP + 8 + 2 * index);
				file.writeShort(cellLocation + numOfBytes);
			}
			index++;
		}
		//Remove the key cell locations in the header
		file.seek(pageStartFP + 8 + 2 * (searchKey + 1));
		copyBytesLength = pageHeader.getHeaderEndOffset() - (8 + 2 * (searchKey + 1));
		copyBytes = new byte[copyBytesLength];
		file.read(copyBytes);
		file.seek(pageStartFP + 8 + 2 * searchKey);
		file.write(copyBytes);
		//Update the number of cells
		file.seek(pageStartFP + 1);
		file.writeByte(pageHeader.getNumCells() - 1);
		//Update cell offset
		file.writeShort(pageHeader.getCellContentStartOffset() + numOfBytes);

		file.seek(pageStartFP + searchKeyCellLocation + 2);
		return pk;
	}

	private void updateRightMostChildRightPointer(RandomAccessFile file, int pageNumber, int rightSiblingPageNumber) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		if(pageHeader.getPageType() == 0x05) {
			updateRightMostChildRightPointer(file, pageHeader.getRightChiSibPointer(), rightSiblingPageNumber);
		} else if(pageHeader.getPageType() == 0x0D) {
			file.seek(pageHeader.getPageStartFP() + 4);
			file.writeInt(rightSiblingPageNumber);
		} else {
			logMessage("Something is wrong invalid page traversed while updating right most child pointer!");
		}
	}

	public DeleteResult traverseAndDeleteCatalog(RandomAccessFile file, int pageNumber, Table table, String tableName, String dbName) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		DeleteResult deleteResult = new DeleteResult();
		if(pageHeader.getNumCells() == 0)
			return deleteResult;
		if(pageHeader.getPageType() == 0x05) {
			//Current node is table interior
			return traverseAndDeleteCatalogInterior(file, pageNumber, pageHeader, table, tableName, dbName);
		} else if(pageHeader.getPageType() == 0x0D) {
			return traverseAndDeleteCatalogLeaf(file, pageNumber, pageHeader, table, tableName, dbName);
		} else {
			logMessage("Invalid Page");
			return deleteResult;
		}
	}

	private DeleteResult traverseAndDeleteCatalogInterior(RandomAccessFile file, int pageNumber, PageHeader pageHeader, Table table, String tableName, String dbName) throws IOException {
		DeleteResult deleteResult = new DeleteResult();
		deleteResult.setLeaf(false);
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		int numCellsDeleted = 0;
		int cellLocationsSize = cellLocations.size();
		if(cellLocationsSize == 0) {
			logMessage("Something went wrong, no cells in this page but want to delete.");
			return deleteResult;
		}
		for (int i = 0; i < cellLocationsSize; i++) {
			//Delete Record and update page header.
			DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(i - numCellsDeleted), false);
			DeleteResult subDeleteResult = traverseAndDeleteCatalog(file, dataCellPage.getLeftChildPointer(), table, tableName, dbName);
			deleteResult = updateDeleteAfterTraversal(file, i - numCellsDeleted, pageHeader, pageNumber, deleteResult, subDeleteResult, dataCellPage.getKey());
			if(subDeleteResult.isWholePageDeleted())
				numCellsDeleted++;
			pageHeader = new PageHeader(file, pageNumber);
			cellLocations = pageHeader.getCellLocations();
		}
		DeleteResult subDeleteResult = traverseAndDeleteCatalog(file, pageHeader.getRightChiSibPointer(), table, tableName, dbName);
		deleteResult = updateDeleteAfterTraversal(file, -1, pageHeader, pageNumber, deleteResult, subDeleteResult, -1);
		return deleteResult;
	}

	


}
