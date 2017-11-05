package com.marvelbase.Commands;

import com.marvelbase.*;
import com.marvelbase.DataType.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.marvelbase.MarvelBase.displayError;
import static com.marvelbase.MarvelBase.logMessage;

public class SelectCommand implements Command{

	private String command = null;
	private String currentDb;
	private SelectParams selectParams;

	public SelectCommand(String command, String currentDb) {
		this.command = command;
		this.currentDb = currentDb;
	}

	public SelectCommand(SelectParams selectParams) {
		this.selectParams = selectParams;
	}

	@Override
	public boolean execute() {
		if(command == null) {
			displayError("Command not initialized");
			return false;
		}
		return parseSelectString();
	}

	public int executeTraverseAndSelect() throws IOException {
		if(selectParams == null)
			return 0;
		return traverseAndSelect(selectParams.getFile(), selectParams.getPageNumber(), selectParams.getSelectColumns(), selectParams.getTable(), selectParams.getCondition(), selectParams.isCheckCondition());
	}
	
	private int getLeftPointerPage(RandomAccessFile file, PageHeader pageHeader, Condition condition) throws IOException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		short left = 0, right = (short) (cellLocations.size() - 1), mid;
		DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), false);
		if(condition.value.greater(dataCellPage.getKey()))
			return pageHeader.getRightChiSibPointer();
		else if(condition.value.equal(dataCellPage.getKey())) {
			switch (condition.operation) {
				case ">=":
					return dataCellPage.getLeftChildPointer();
				case "=":
					return dataCellPage.getLeftChildPointer();
				case ">":
					return pageHeader.getRightChiSibPointer();
				default:
					logMessage("This function should not be called for this type.");
					return -1;
			}
		}

		while(left != right) {
			mid = (short) ((left + right) / 2);
			dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), false);
			if(condition.value.greater(dataCellPage.getKey()))
				left = (short) (mid + 1);
			if(condition.value.lesser(dataCellPage.getKey()))
				right = mid;
			else {
				switch (condition.operation) {
					case ">=":
						return dataCellPage.getLeftChildPointer();
					case "=":
						return dataCellPage.getLeftChildPointer();
					case ">":
						dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid + 1), false);
						return dataCellPage.getLeftChildPointer();
					default:
						logMessage("This function should not be called for this type.");
						return -1;
				}
			}
		}
		return new DataCellPage(file, pageStartPointer + cellLocations.get(left), false).getLeftChildPointer();
	}


	private boolean parseSelectString() {
		Pattern pattern = Pattern.compile("^select (.+) from (\\S+)(?: where (.+)$|$)");
		Matcher matcher = pattern.matcher(this.command);
		if(matcher.find()) {
			if(matcher.groupCount() == 3) {
				return checkAndExecuteSelectString(matcher.group(1), matcher.group(2), matcher.group(3));
			} else {
				CommandHelper.wrongSyntax();
				return false;
			}
		} else {
			CommandHelper.wrongSyntax();
			return false;
		}
	}
	
	private void displayRecord(RandomAccessFile file, long location, Table table, List<Column> selectColumns) throws IOException {
		file.seek(location);
		List<Integer> positions = new ArrayList<>();
		if(selectColumns != null) {
			for (Column column : selectColumns) {
				positions.add(column.getOrdinalPosition());
			}
		}
		HashMap<String, String> values = new HashMap<>();
		short payloadLength = file.readShort();
		if(payloadLength < 1) {
			logMessage("Payload empty. Please check again.");
		}
		Column pkColumn = table.getPkColumn();
		int pk = file.readInt();
		int pkPosition = pkColumn.getOrdinalPosition();
		if(positions.contains(pkPosition))
			values.put(pkColumn.getName(), pk + "");
		byte numOfColumns = file.readByte();
		if(numOfColumns < 1)
			logMessage("Payload empty. Please check again.");
		for(Map.Entry<String, Column> entry : table.getColumns().entrySet()) {
			Column column = entry.getValue();
			if(!column.isPk()) {
				byte typeByte = file.readByte();
				int numBytes = UtilityTools.getNumberOfBytesFromTypebyte(typeByte);
				if(positions.contains(column.getOrdinalPosition())) {
					byte[] byteArray = new byte[numBytes];
					file.read(byteArray);
					if(UtilityTools.valueNull(typeByte))
						values.put(column.getName(), "NULL");
					else
						values.put(column.getName(), column.getRecordValue(byteArray));
				} else {
					file.skipBytes(numBytes);
				}
			}
		}

		System.out.println("");
		if(selectColumns != null) {
			for (Column column : selectColumns) {
				System.out.print("|\t");
				System.out.format(column.getFormat(), values.get(column.getName()));
				System.out.print("\t|");
			}
		}
	}


	private boolean checkAndExecuteSelectString(String selectColumns, String dbTableName, String condition) {
		//String[] tableSplit = tableName.split("\\.", 1);
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
			dbName = this.currentDb;
			tableName = dbTableName;
		}
		File dbFile = new File("data/" + dbName);
		if(!dbFile.exists() || !dbFile.isDirectory()) {
			displayError("Database does not exist.");
			return false;
		}
		String fileName = "data/" + dbName + "/" + tableName + ".tbl";
		File f = new File(fileName);
		if (!f.exists()) {
			displayError("Table does not exist.");
			return false;
		}
		try {
			RandomAccessFile sBColumnsTableFile = new RandomAccessFile("data/catalog/mb_columns.tbl", "rw");
			List<Column> columns = CommandHelper.traverseAndGetColumns(sBColumnsTableFile, 0, UtilityTools.sbColumnsTable, tableName, dbName);
			sBColumnsTableFile.close();
			columns.sort((column, t1) -> new Integer(column.getOrdinalPosition()).compareTo(t1.getOrdinalPosition()));
			LinkedHashMap<String, Column> columnHashMap = new LinkedHashMap<>();
			for (Column column: columns) {
				columnHashMap.put(column.getName(), column);
			}
			List<Column> selectColumnsList = new ArrayList<>();
			if(selectColumns.trim().equals("*")) {
				selectColumnsList.addAll(columns);
			} else {
				for (String columnName : selectColumns.split(",")) {
					if (columnHashMap.get(columnName) != null)
						selectColumnsList.add(columnHashMap.get(columnName));
					else {
						CommandHelper.wrongSyntax();
						return false;
					}
				}
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
				} else {
					displayError("Where condition problem.");
					return false;
				}
			}
			CommandHelper.displayTableHeader(selectColumnsList);
			traverseAndSelect(tableFile, 0, selectColumnsList, table, conditionObj, true);
			System.out.println();
			return true;
		} catch (IOException | ParseException e) {
			e.printStackTrace();
			return false;
		}
	}

	private int traverseAndSelect(RandomAccessFile file, int pageNumber, List<Column> selectColumns, Table table, Condition condition, boolean checkCondition) throws IOException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		if(condition != null && condition.column.isPk() && condition.operation.equals("is null"))
			return 0;
		if(condition != null && condition.column.isPk() && condition.operation.equals("is not null"))
			condition = null;
		if(pageHeader.getPageType() == 0x05) {
			//Current node is table interior
			if(condition != null && condition.column.isPk() && (condition.operation.equals(">") || condition.operation.equals(">=") || condition.operation.equals("="))) {
				//If operation is > or >= or = get pointer page to traverse.
				int leftPointerPage = getLeftPointerPage(file, pageHeader, condition);
				return traverseAndSelect(file, leftPointerPage, selectColumns, table, condition, true);
			} else {
				if(pageHeader.getNumCells() == 0)
					return 0;
				//Traverse to the left most cell location.
				DataCellPage dataCellPage = new DataCellPage(file, pageHeader.getPageStartFP() + pageHeader.getCellLocations().get(0), false);
				return traverseAndSelect(file, dataCellPage.getLeftChildPointer(), selectColumns, table, condition, true);
			}
		} else if (pageHeader.getPageType() == 0x0D) {
			//Current node is table leaf.
			List<Short> cellLocations = pageHeader.getCellLocations();
			long pageStartPointer = pageHeader.getPageStartFP();
			if(pageHeader.getNumCells() == 0)
				return 0;
			if(condition != null && condition.column.isPk() && (condition.operation.equals(">") || condition.operation.equals(">=") || condition.operation.equals("=") || condition.operation.equals("<") || condition.operation.equals("<="))) {
				if(condition.operation.equals("=")) {
					int searchKey = CommandHelper.binarySearchKey(file, pageHeader, condition);
					if (searchKey == -1)
						return 0;
					displayRecord(file, pageStartPointer + cellLocations.get(searchKey), table, selectColumns);
					return 1;
				} else if(condition.operation.equals("<") || condition.operation.equals("<=")) {
					int numSelectedRows = 0;
					int key = CommandHelper.largestKeyLesserEqual(file, pageHeader, condition, true);
					if(key == -1) {
						return 0;
					}
					if(condition.operation.equals("<")) {
						DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), true);
						if(condition.value.equal(dataCellPage.getKey()))
							key--;
					}
					for (int i = 0; i <= key; i++) {
						displayRecord(file, pageStartPointer + cellLocations.get(i), table, selectColumns);
						numSelectedRows++;
					}
					if(key == cellLocations.size() - 1 && pageHeader.getRightChiSibPointer() != -1)
						numSelectedRows += traverseAndSelect(file, pageHeader.getRightChiSibPointer(), selectColumns, table, condition, false);
					return numSelectedRows;
				} else {
					int numSelectedRows = 0;
					if(!checkCondition) {
						for (Short cellLocation : cellLocations) {
							displayRecord(file, pageStartPointer + cellLocation, table, selectColumns);
							numSelectedRows++;
						}
					} else {
						int key = CommandHelper.smallestKeyGreaterEqual(file, pageHeader, condition, true);
						if(key == -1) {
							logMessage("Something went wrong. skGE is -1. Should not be in this page.");
							return 0;
						}
						if(condition.operation.equals(">")) {
							DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(key), true);
							if(condition.value.equal(dataCellPage.getKey()))
								key++;
						}
						for (int i = key; i < cellLocations.size(); i++) {
							displayRecord(file, pageStartPointer + cellLocations.get(i), table, selectColumns);
							numSelectedRows++;
						}
					}
					if(pageHeader.getRightChiSibPointer() != -1)
						numSelectedRows += traverseAndSelect(file, pageHeader.getRightChiSibPointer(), selectColumns, table, condition, false);
					return numSelectedRows;
				}
			} else {
				int numSelectedRows = 0;
				int columnIndex = -1;
				if(condition != null && !condition.column.isPk()) {
					columnIndex = table.getPkColumn().getOrdinalPosition() < condition.column.getOrdinalPosition() ? condition.column.getOrdinalPosition() - 1 : condition.column.getOrdinalPosition();
				}
				boolean conditionColumnIsPk = condition != null && condition.column.isPk();
				int index = 0;
				for (Short cellLocation : cellLocations) {
					file.seek(pageStartPointer + cellLocation);
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
								return 0;
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
					displayRecord(file, pageStartPointer + cellLocation, table, selectColumns);
					numSelectedRows++;
				}
				if(pageHeader.getRightChiSibPointer() != -1)
					numSelectedRows += traverseAndSelect(file, pageHeader.getRightChiSibPointer(), selectColumns, table, condition, false);
				return numSelectedRows;
			}
		} else {
			logMessage("Incorrect Page type");
			return 0;
		}
	}

	
	

}
