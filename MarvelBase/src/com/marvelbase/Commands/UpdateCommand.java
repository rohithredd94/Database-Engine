package com.marvelbase.Commands;

import com.marvelbase.*;
import com.marvelbase.DataType.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.marvelbase.MarvelBase.displayError;
import static com.marvelbase.MarvelBase.response;
import static com.marvelbase.MarvelBase.logMessage;

public class UpdateCommand implements Command {

	private String command = null;
	private String currentDb;

	public UpdateCommand(String command, String currentDb) {
		this.command = command;
		this.currentDb = currentDb;
	}

	@Override
	public boolean execute() {
		if(this.command == null) {
			displayError("Command not initialized");
			return false;
		}
		return parseUpdateString();
	}
	
	//Returns integrity violated or not
		private boolean atomicTraverseAndUpdate(RandomAccessFile file, Table table, Condition condition, LinkedHashMap<String, String> values) throws IOException, ParseException {
			file.seek(0);
			long oldFileLength = file.length();
			byte[] bytes = new byte[(int) oldFileLength];
			file.readFully(bytes);
			UpdateResult updateResult = traverseAndUpdate(file, 0, table, condition, values, -1, -1, new ArrayList<>());
			if(updateResult.isIntegrityViolated()) {
				file.seek(0);
				file.setLength(oldFileLength);
				file.write(bytes);
				return true;
			} else
				return false;
		}
		
		private short getNextIndex(RandomAccessFile file, PageHeader pageHeader, int lastUpdatedPk) throws IOException {

			short left = 0, right = (short) (pageHeader.getNumCells() - 1), mid;
			long pageStartPointer = pageHeader.getPageStartFP();
			List<Short> cellLocations = pageHeader.getCellLocations();
			DataCellPage rightDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), true);
			DataCellPage leftDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(left), true);
			if(lastUpdatedPk >= rightDataCellPage.getKey())
				return -1;
			else if (lastUpdatedPk < leftDataCellPage.getKey())
				return left;

			DataCellPage dataCellPage;
			while(left != right) {
				mid = (short) ((left + right) / 2);
				dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), true);
				if(lastUpdatedPk > dataCellPage.getKey())
					left = (short) (mid + 1);
				else if (lastUpdatedPk < dataCellPage.getKey())
					right = (short) (mid - 1);
				else
					return (short) (mid + 1);
			}
			return left;
		}

	private boolean parseUpdateString() {
		final String regex = "where(?=([^\"\\\\]*(\\\\.|\"([^\"\\\\]*\\\\.)*[^\"\\\\]*\"))*[^\"]*$)";
		final String updateWithoutWhereRegex = "^update (\\S*) set (.+)$";
		String[] stringSplit = this.command.split(regex);
		String whereString = null;
		String updateStringWithoutWhere;
		if(stringSplit.length == 2) {
			updateStringWithoutWhere = stringSplit[0];
			whereString = stringSplit[1];
		} else if (stringSplit.length == 1) {
			updateStringWithoutWhere = stringSplit[0];
		} else {
			displayError("There cannot be more than one where keywords in the command.");
			return false;
		}
		Pattern updateWithoutWherePattern = Pattern.compile(updateWithoutWhereRegex);
		final Matcher matcher = updateWithoutWherePattern.matcher(updateStringWithoutWhere.trim());
		if(matcher.find()) {
			String dbTableName = matcher.group(1);
			String setString = matcher.group(2);
			return checkAndExecuteUpdateString(dbTableName, setString, whereString);
		} else {
			CommandHelper.wrongSyntax();
			return false;
		}
	}
	
	private UpdateResult traverseAndUpdateInterior(RandomAccessFile file, int pageNumber, PageHeader pageHeader, Table table, Condition condition, LinkedHashMap<String, String> values, int lui, int lupk, List<Integer> insertedPks) throws IOException, ParseException {
		UpdateResult updateResult = new UpdateResult();
		//Page is not leaf
		updateResult.setLeaf(false);
		long pageStartPointer = pageHeader.getPageStartFP();
		List<Integer> insertedPksTemp = new ArrayList<>();
		insertedPksTemp.addAll(insertedPks);
		updateResult.setNewPksInserted(new ArrayList<>());
		UpdateResult subUpdateResult;
		int index;
		List<Short> cellLocations = pageHeader.getCellLocations();
		if(condition != null && condition.column.isPk() && (condition.operation.equals(">") || condition.operation.equals(">=") || condition.operation.equals("=") || condition.operation.equals("<") || condition.operation.equals("<="))) {
			if(condition.operation.equals("=")) {
				boolean integrityViolated = false;
				int lastUpdatedPk = lupk;
				index = CommandHelper.smallestKeyGreaterEqual(file, pageHeader, condition, false);
				if(index == -1) {
					subUpdateResult = traverseAndUpdate(file, pageHeader.getRightChiSibPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
				} else {
					subUpdateResult = traverseAndUpdate(file, new DataCellPage(file, cellLocations.get(index), false).getLeftChildPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
				}
				integrityViolated = subUpdateResult.isIntegrityViolated();
				updateResult.setIntegrityViolated(integrityViolated);
				insertedPksTemp.addAll(subUpdateResult.getNewPksInserted());
				updateResult.addNewPksInserted(subUpdateResult.getNewPksInserted());
			} else if(condition.operation.equals(">=") || condition.operation.equals(">")) {
				boolean integrityViolated = false, pageCompletelyUpdated = false;
				int lastUpdatedPk = lupk;
				while(!integrityViolated && !pageCompletelyUpdated) {
					if(lastUpdatedPk == -1) {
						index = CommandHelper.smallestKeyGreaterEqual(file, pageHeader, condition, false);
						if(condition.operation.equals(">")) {
							DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(index), true);
							if(condition.value.equal(dataCellPage.getKey())) {
								index++;
								if(index >= pageHeader.getNumCells())
									index = -1;
							}
						}
					} else {
						index = getNextInteriorUpdateIndex(file, pageHeader, lastUpdatedPk);
					}
					if(index == -1) {
						pageCompletelyUpdated = true;
						subUpdateResult = traverseAndUpdate(file, pageHeader.getRightChiSibPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
					} else {
						subUpdateResult = traverseAndUpdate(file, new DataCellPage(file, cellLocations.get(index), false).getLeftChildPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
					}
					//UpdateNextResult updateNextResult = traverseAndUpdateOnLastUpdatedInterior(file, lastUpdatedPk, lastUpdatedIndex, true, pageHeader, pageNumber, insertedPks, table, condition, values);
					insertedPksTemp.addAll(subUpdateResult.getNewPksInserted());
					updateResult.addNewPksInserted(subUpdateResult.getNewPksInserted());
					integrityViolated = subUpdateResult.isIntegrityViolated();
					lastUpdatedPk = subUpdateResult.getLastUpdatedPk();
					pageHeader = new PageHeader(file, pageNumber);
					cellLocations = pageHeader.getCellLocations();
				}
				updateResult.setIntegrityViolated(integrityViolated);
			} else {
				//Operation is < or <=
				boolean integrityViolated = false, pageCompletelyUpdated = false, conditionFailed = false;
				int lastUpdatedPk = lupk;
				while(!integrityViolated && !pageCompletelyUpdated && !conditionFailed) {
					if(lastUpdatedPk == -1) {
						index = 0;
					} else {
						index = getNextInteriorUpdateIndex(file, pageHeader, lastUpdatedPk);
					}
					if(index == -1) {
						pageCompletelyUpdated = true;
						subUpdateResult = traverseAndUpdate(file, pageHeader.getRightChiSibPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
					} else {
						subUpdateResult = traverseAndUpdate(file, new DataCellPage(file, cellLocations.get(index), false).getLeftChildPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
					}
					//UpdateNextResult updateNextResult = traverseAndUpdateOnLastUpdatedInterior(file, lastUpdatedPk, lastUpdatedIndex, true, pageHeader, pageNumber, insertedPks, table, condition, values);
					insertedPksTemp.addAll(subUpdateResult.getNewPksInserted());
					updateResult.addNewPksInserted(subUpdateResult.getNewPksInserted());
					integrityViolated = subUpdateResult.isIntegrityViolated();
					conditionFailed = subUpdateResult.isConditionFailed();
					lastUpdatedPk = subUpdateResult.getLastUpdatedPk();
					pageHeader = new PageHeader(file, pageNumber);
					cellLocations = pageHeader.getCellLocations();
				}
				updateResult.setIntegrityViolated(integrityViolated);
				updateResult.setConditionFailed(conditionFailed);
			}
		} else {
			boolean integrityViolated = false, pageCompletelyUpdated = false;
			int lastUpdatedPk = lupk;
			while(!integrityViolated && !pageCompletelyUpdated) {
				index = getNextInteriorUpdateIndex(file, pageHeader, lastUpdatedPk);
				if(index == -1) {
					pageCompletelyUpdated = true;
					subUpdateResult = traverseAndUpdate(file, pageHeader.getRightChiSibPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
				} else {
					subUpdateResult = traverseAndUpdate(file, new DataCellPage(file, cellLocations.get(index), false).getLeftChildPointer(), table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
				}
				//UpdateNextResult updateNextResult = traverseAndUpdateOnLastUpdatedInterior(file, lastUpdatedPk, lastUpdatedIndex, true, pageHeader, pageNumber, insertedPks, table, condition, values);

				insertedPksTemp.addAll(subUpdateResult.getNewPksInserted());
				updateResult.addNewPksInserted(subUpdateResult.getNewPksInserted());
				integrityViolated = subUpdateResult.isIntegrityViolated();
				lastUpdatedPk = subUpdateResult.getLastUpdatedPk();
				pageHeader = new PageHeader(file, pageNumber);
				cellLocations = pageHeader.getCellLocations();
			}
			updateResult.setIntegrityViolated(integrityViolated);
		}
		return updateResult;
	}

	private boolean checkAndExecuteUpdateString(String dbTableName, String setString, String condition) {
		if(condition != null)
			condition = condition.trim();
		setString = setString.trim();
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
		if(dbName.equalsIgnoreCase("catalog")) {
			displayError("You cannot update records inside catalog.");
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
			Table table = new Table(dbName, tableName, columnHashMap);
			RandomAccessFile tableFile = new RandomAccessFile(table.getFilePath(), "rw");
			LinkedHashMap<String, String> values = new LinkedHashMap<>();
			final String commaRegex = ",(?=([^\"\\\\]*(\\\\.|\"([^\"\\\\]*\\\\.)*[^\"\\\\]*\"))*[^\"]*$)";
			String[] valuesString = setString.split(commaRegex);
			Pattern setPattern = Pattern.compile("^(\\S+) = (.*)$");
			for (String valueString : valuesString) {
				valueString = valueString.trim();
				Matcher matcher = setPattern.matcher(valueString);
				if(matcher.find()) {
					if (matcher.groupCount() != 2) {
						CommandHelper.wrongSyntax();
						return false;
					}
					Column column = columnHashMap.get(matcher.group(1));
					if(column == null) {
						displayError("Set Column not found");
						return false;
					}
					DataType columnValue = column.getColumnValue(matcher.group(2));
					if(columnValue instanceof TextType || columnValue instanceof DateTimeType || columnValue instanceof DateType) {
						if(!UtilityTools.regexSatisfy((String) columnValue.getValue(), "^\".*\"$") && !UtilityTools.regexSatisfy((String) columnValue.getValue(), "^'.*'$")) {
							displayError("String, dates, datetime columns must be string and must be in '' or \"\"");
							return false;
						}
					}
					values.put(matcher.group(1).trim(), matcher.group(2).trim().replaceAll("^\"|\"$", "").replaceAll("^'|'$", ""));
				} else {
					CommandHelper.wrongSyntax();
					return false;
				}
			}
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
			boolean integrityViolated = atomicTraverseAndUpdate(tableFile, table, conditionObj, values);
			response(integrityViolated ? "Integrity violated. The command did not change the state of the database." : "Updated successfully");
			return !integrityViolated;
		} catch (IOException | ParseException e) {
			e.printStackTrace();
			return false;
		}
	}

	private UpdateResult traverseAndUpdate(RandomAccessFile file, int pageNumber, Table table, Condition condition, LinkedHashMap<String, String> values, int lui, int lupk, List<Integer> insertedList) throws IOException, ParseException {
		PageHeader pageHeader = new PageHeader(file, pageNumber);
		UpdateResult updateResult = new UpdateResult();
		if(condition != null && condition.column.isPk() && condition.operation.equals("is null"))
			return updateResult;
		if(condition != null && condition.column.isPk() && condition.operation.equals("is not null"))
			condition = null;
		if(pageHeader.getNumCells() == 0)
			return updateResult;

		if(pageHeader.getPageType() == 0x05) {
			//Current node is table interior
			return traverseAndUpdateInterior(file, pageNumber, pageHeader, table, condition, values, lui, lupk, insertedList);
		} else if(pageHeader.getPageType() == 0x0D) {
			//Current node is table Leaf
			return traverseAndUpdateLeaf(file, pageNumber, pageHeader, table, condition, values, lui, lupk, insertedList);
		} else {
			logMessage("Invalid Page");
			return updateResult;
		}
	}

	private UpdateNextResult traverseAndUpdateOnLastUpdatedLeaf(RandomAccessFile file, int lastUpdatedPk, int lastUpdatedIndex, boolean checkCondition, PageHeader pageHeader, int pageNumber, List<Integer> newlyInsertedPks, Table table, Condition condition, LinkedHashMap<String, String> values) throws IOException, ParseException {
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		if(lastUpdatedIndex != -1 && lastUpdatedIndex > pageHeader.getNumCells()) {
			//If last updated index is not -1 and it is greater than the num of cells means that the next key/Old key is in another page is in another page.
			return new UpdateNextResult(lastUpdatedPk, -1);
		} else if(lastUpdatedIndex == -1 && lastUpdatedPk == -1) {
			//If last update index is -1 and last updated pk is -1, then check the condition
			return updateOnIndexLeaf(file, 0, checkCondition, pageHeader, pageNumber, newlyInsertedPks, table, condition, values);
		} else if(lastUpdatedIndex == -1) {
			//get the next index through last updated pk.
			int nextIndex = getNextIndex(file, pageHeader, lastUpdatedPk);
			if(nextIndex != -1)
				return updateOnIndexLeaf(file, nextIndex, checkCondition, pageHeader, pageNumber, newlyInsertedPks, table, condition, values);
			else
				return new UpdateNextResult(lastUpdatedPk, -1);
		} else if(lastUpdatedPk != -1) {
			DataCellPage currentInLastUpdatedIndex = new DataCellPage(file, pageStartPointer + cellLocations.get(lastUpdatedIndex), true);
			if(currentInLastUpdatedIndex.getKey() == lastUpdatedPk && lastUpdatedIndex + 1 < pageHeader.getNumCells())
				return updateOnIndexLeaf(file, lastUpdatedIndex + 1, checkCondition, pageHeader, pageNumber, newlyInsertedPks, table, condition, values);
			else if(lastUpdatedPk == currentInLastUpdatedIndex.getKey())
				return new UpdateNextResult(lastUpdatedPk, -1);
			else if(currentInLastUpdatedIndex.getKey() > lastUpdatedPk)
				return updateOnIndexLeaf(file, lastUpdatedIndex, checkCondition, pageHeader, pageNumber, newlyInsertedPks, table, condition, values);
			else {
				//something is wrong
				logMessage("Something wrong. should not be here. current pk < lupk");
				return new UpdateNextResult(lastUpdatedPk, -1, true);
			}
		} else {
			//something is wrong
			logMessage("Something wrong. should not be here.");
			return new UpdateNextResult(lastUpdatedPk, -1, true);
		}
	}

	private UpdateResult traverseAndUpdateLeaf(RandomAccessFile file, int pageNumber, PageHeader pageHeader, Table table, Condition condition, LinkedHashMap<String, String> values, int lui, int lupk, List<Integer> insertedPks) throws IOException, ParseException {
		long pageStartPointer = pageHeader.getPageStartFP();
		UpdateResult updateResult = new UpdateResult();
		//Page is leaf
		updateResult.setLeaf(true);
		List<Short> cellLocations = pageHeader.getCellLocations();
		List<Integer> insertedPksTemp = new ArrayList<>();
		insertedPksTemp.addAll(insertedPks);
		if(condition != null && condition.column.isPk() && (condition.operation.equals(">") || condition.operation.equals(">=") || condition.operation.equals("=") || condition.operation.equals("<") || condition.operation.equals("<="))) {
			if(condition.operation.equals("=")) {
				int searchKey = CommandHelper.binarySearchKey(file, pageHeader, condition);
				if (searchKey == -1)
					return updateResult;
				//Update Record and update page header.
				SplitPage updateSplitPage = updateRecord(file, searchKey, pageNumber, pageHeader, table, values);
				if(updateSplitPage.isInserted()) {
					updateResult.keyChange(((IntType)condition.getValue()).getValue());
					updateResult.keyInserted(updateSplitPage.getKey());
				} else if(updateSplitPage.getKey() == -1) {
					//If a new record could not be inserted cause of integrity violation, roll back to the original file.
					logMessage("New record could not be inserted cause of integrity violation");
					updateResult.setIntegrityViolated(true);
				} else {
					updateResult.setLastUpdatedPk(((IntType)condition.getValue()).getValue());
				}
			} else if(condition.operation.equals("<") || condition.operation.equals("<=")) {
				boolean conditionFailed = false, integrityViolated = false, pageCompletelyUpdated = false, pageChangedToInterior = false;
				int lastUpdatedPk = lupk, lastUpdatedIndex = lui;
				while(!conditionFailed && !integrityViolated && !pageCompletelyUpdated) {
					pageHeader = new PageHeader(file, pageNumber);
					if(pageHeader.getPageType() == 0x05) {
						pageChangedToInterior = true;
						break;
					} else if (pageHeader.getPageType() == 0x00) {
						break;
					}
					UpdateNextResult updateNextResult = traverseAndUpdateOnLastUpdatedLeaf(file, lastUpdatedPk, lastUpdatedIndex, true, pageHeader, pageNumber, insertedPksTemp, table, condition, values);
					conditionFailed = updateNextResult.isConditionFailed();
					integrityViolated = updateNextResult.isIntegrityViolated();
					pageCompletelyUpdated = updateNextResult.getLastUpdatedIndex() == -1;
					lastUpdatedIndex = updateNextResult.getLastUpdatedIndex();
					if(updateNextResult.getNewInsertedPk() != -1) {
						updateResult.keyChange(updateNextResult.getLastUpdatedPk());
						updateResult.keyInserted(updateNextResult.getNewInsertedPk());
						insertedPksTemp.add(updateNextResult.getNewInsertedPk());
					} else {
						lastUpdatedPk = updateNextResult.getLastUpdatedPk();
					}
				}
				if(pageChangedToInterior) {
					UpdateResult updateResult1 = traverseAndUpdate(file, pageNumber, table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
					conditionFailed = updateResult1.isConditionFailed();
					integrityViolated = updateResult1.isIntegrityViolated();
					lastUpdatedPk = updateResult1.getLastUpdatedPk();
					updateResult.addNewPksInserted(updateResult1.getNewPksInserted());
				}
				updateResult.setLastUpdatedPk(lastUpdatedPk);
				updateResult.setIntegrityViolated(integrityViolated);
				updateResult.setConditionFailed(conditionFailed);
			} else {
				if(lupk == -1) {
					lui = CommandHelper.smallestKeyGreaterEqual(file, pageHeader, condition, true);
					if (lui == -1) {
						logMessage("Something went wrong. skGE is -1. Should not be in this page.");
						updateResult.setIntegrityViolated(true);
						return updateResult;
					}
					if (condition.operation.equals(">")) {
						DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(lui), true);
						if (condition.value.equal(dataCellPage.getKey()))
							lui++;
					}
					lui--;
					if(lui == -1) {
						lupk = -1;
					} else {
						DataCellPage dataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(lui), true);
						lupk = dataCellPage.getKey();
					}
				}
				logMessage("LUPK: " + lupk);
				boolean integrityViolated = false, pageCompletelyUpdated = false, pageChangedToInterior = false;
				int lastUpdatedPk = lupk, lastUpdatedIndex = lui;
				while(!integrityViolated && !pageCompletelyUpdated) {
					if(pageHeader.getPageType() == 0x05) {
						pageChangedToInterior = true;
						break;
					} else if (pageHeader.getPageType() == 0x00) {
						break;
					}
					UpdateNextResult updateNextResult = traverseAndUpdateOnLastUpdatedLeaf(file, lastUpdatedPk, lastUpdatedIndex, false, pageHeader, pageNumber, insertedPksTemp, table, condition, values);
					pageHeader = new PageHeader(file, pageNumber);
					integrityViolated = updateNextResult.isIntegrityViolated();
					pageCompletelyUpdated = updateNextResult.getLastUpdatedIndex() == -1;
					lastUpdatedIndex = updateNextResult.getLastUpdatedIndex();
					if(updateNextResult.getNewInsertedPk() != -1) {
						updateResult.keyChange(updateNextResult.getLastUpdatedPk());
						updateResult.keyInserted(updateNextResult.getNewInsertedPk());
						insertedPksTemp.add(updateNextResult.getNewInsertedPk());
					} else {
						lastUpdatedPk = updateNextResult.getLastUpdatedPk();
					}
				}
				if(pageChangedToInterior) {
					UpdateResult updateResult1 = traverseAndUpdate(file, pageNumber, table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
					integrityViolated = updateResult1.isIntegrityViolated();
					lastUpdatedPk = updateResult1.getLastUpdatedPk();
					updateResult.addNewPksInserted(updateResult1.getNewPksInserted());
				}
				updateResult.setIntegrityViolated(integrityViolated);
				updateResult.setLastUpdatedPk(lastUpdatedPk);
			}
		} else {
			boolean integrityViolated = false, pageCompletelyUpdated = false, pageChangedToInterior = false;
			int lastUpdatedPk = lupk, lastUpdatedIndex = lui;
			while(!integrityViolated && !pageCompletelyUpdated) {
				if(pageHeader.getPageType() == 0x05) {
					pageChangedToInterior = true;
					break;
				} else if (pageHeader.getPageType() == 0x00) {
					break;
				}
				UpdateNextResult updateNextResult = traverseAndUpdateOnLastUpdatedLeaf(file, lastUpdatedPk, lastUpdatedIndex, true, pageHeader, pageNumber, insertedPksTemp, table, condition, values);
				pageHeader = new PageHeader(file, pageNumber);
				integrityViolated = updateNextResult.isIntegrityViolated();
				pageCompletelyUpdated = updateNextResult.getLastUpdatedIndex() == -1;
				lastUpdatedIndex = updateNextResult.getLastUpdatedIndex();
				if(updateNextResult.getNewInsertedPk() != -1) {
					updateResult.keyChange(updateNextResult.getLastUpdatedPk());
					updateResult.keyInserted(updateNextResult.getNewInsertedPk());
					insertedPksTemp.add(updateNextResult.getNewInsertedPk());
				} else {
					lastUpdatedPk = updateNextResult.getLastUpdatedPk();
				}
			}
			if(pageChangedToInterior) {
				logMessage("Page changed to interior");
				UpdateResult updateResult1 = traverseAndUpdate(file, pageNumber, table, condition, values, -1, lastUpdatedPk, insertedPksTemp);
				integrityViolated = updateResult1.isIntegrityViolated();
				lastUpdatedPk = updateResult1.getLastUpdatedPk();
				updateResult.addNewPksInserted(updateResult1.getNewPksInserted());
			}
			updateResult.setIntegrityViolated(integrityViolated);
			updateResult.setLastUpdatedPk(lastUpdatedPk);
		}
		return updateResult;
	}


	private UpdateNextResult updateOnIndexLeaf(RandomAccessFile file, int index, boolean checkCondition, PageHeader pageHeader, int pageNumber, List<Integer> newlyInsertedPks, Table table, Condition condition, LinkedHashMap<String, String> values) throws IOException, ParseException {
		long pageStartPointer = pageHeader.getPageStartFP();
		int columnIndex = -1;
		if(condition != null && !condition.column.isPk()) {
			columnIndex = table.getPkColumn().getOrdinalPosition() < condition.column.getOrdinalPosition() ? condition.column.getOrdinalPosition() - 1 : condition.column.getOrdinalPosition();
		}
		boolean conditionColumnIsPk = condition != null && condition.column.isPk();
		file.seek(pageStartPointer + pageHeader.getCellLocations().get(index));
		file.skipBytes(2);
		int pk = file.readInt();
		if(newlyInsertedPks.contains(pk))
			return new UpdateNextResult(pk, index);
		if(checkCondition && condition != null) {
			if(conditionColumnIsPk) {
				if(!condition.result(new IntType(pk)))
					return new UpdateNextResult(pk, index, false, true);
			} else {
				file.seek(pageStartPointer + pageHeader.getCellLocations().get(index));
				file.skipBytes(6);
				int numColumns = file.readByte();
				if (columnIndex >= numColumns) {
					logMessage("Something very wrong happened to the database. Number of columns is less.");
					return new UpdateNextResult(pk, index);
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
							return new UpdateNextResult(pk, index, false, true);
						break;
					case "is not null":
						if (UtilityTools.valueNull(type) || type == 0x0C)
							return new UpdateNextResult(pk, index, false, true);
						break;
					default:
						if (UtilityTools.valueNull(type)) {
							return new UpdateNextResult(pk, index, false, true);
						} else {
							int bytesLength = UtilityTools.getNumberOfBytesFromTypebyte(type);
							byte[] bytes = new byte[bytesLength];
							file.read(bytes);
							DataType tableValue = CommandHelper.getDataTypeFromByteType(type, bytes);
							if (!condition.result(tableValue))
								return new UpdateNextResult(pk, index, false, true);
						}
						break;
				}
			}
		}
		//Update the record
		SplitPage splitPage = updateRecord(file, index, pageNumber, pageHeader, table, values);
		if(splitPage.isInserted()) {
			logMessage("New record is inserted");
			return new UpdateNextResult(pk, index, splitPage.getKey(), pk);
		} else if(splitPage.getKey() == -1) {
			logMessage("New record could not be inserted cause of integrity violation");
			return new UpdateNextResult(-1, -1, true);
		} else {
			return new UpdateNextResult(pk, index);
		}
	}

	private int getNextInteriorUpdateIndex(RandomAccessFile file, PageHeader pageHeader, int lastUpdatedPk) throws IOException {
		//Get the Smallest key greater than the last updated pk.
		List<Short> cellLocations = pageHeader.getCellLocations();
		long pageStartPointer = pageHeader.getPageStartFP();
		int left = 0, right = pageHeader.getNumCells() - 1, mid;
		DataCellPage leftDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(left), false);
		DataCellPage rightDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(right), false);
		if(lastUpdatedPk < leftDataCellPage.getKey())
			return left;
		else if(lastUpdatedPk == leftDataCellPage.getKey())
			return left + 1 < pageHeader.getNumCells() ? left + 1 : -1;
		else if(lastUpdatedPk >= rightDataCellPage.getKey())
			return -1;
		while(left != right) {
			mid = (left + right) / 2;
			DataCellPage midDataCellPage = new DataCellPage(file, pageStartPointer + cellLocations.get(mid), false);
			if(lastUpdatedPk > midDataCellPage.getKey())
				left = mid + 1;
			else if(lastUpdatedPk < midDataCellPage.getKey())
				right = mid;
			else
				return mid + 1 < pageHeader.getNumCells() ? mid + 1 : -1;
		}
		return left;
	}

	private SplitPage updateRecord(RandomAccessFile file, int searchKey, int pageNumber, PageHeader pageHeader, Table table, LinkedHashMap<String, String> values) throws IOException, ParseException {
		long pageStartPointer = pageHeader.getPageStartFP();
		file.seek(pageStartPointer + pageHeader.getCellLocations().get(searchKey));
		int oldRecordLength = file.readShort() + 6;
		file.skipBytes(4);
		int newRecordLength = 7;
		LinkedHashMap<String, Column> columns = table.getColumns();
		Byte numColumns = file.readByte();
		if(numColumns < columns.entrySet().size()) {
			displayError("Num of columns in file does not match with num of columns in catalog.");
			return new SplitPage(-1);
		}
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			Column column = entry.getValue();
			if(!column.isPk()) {
				byte dataType = file.readByte();
				if (!column.isDataTypeCorrect(dataType)) {
					displayError("Datatype is not correct.");
					return new SplitPage(-1);
				}
				if(column.getType().toUpperCase().equals("TEXT") && values.get(column.getName()) != null) {
					newRecordLength += values.get(column.getName()).length() + 1;
				} else
					newRecordLength += UtilityTools.getNumberOfBytesFromTypebyte(dataType) + 1;
				file.skipBytes(UtilityTools.getNumberOfBytesFromTypebyte(dataType));
			}
		}
		ByteBuffer byteBuffer = ByteBuffer.allocate(newRecordLength);
		byteBuffer.putShort((short) (newRecordLength - 6));
		boolean needToDeleteAndUpdate = values.get(table.getPkColumn().getName()) != null;
		file.seek(pageStartPointer + pageHeader.getCellLocations().get(searchKey) + 2);
		int pkValue = file.readInt();
		logMessage("Updating row_id: " + pkValue);
		int newPkValue = needToDeleteAndUpdate ? Integer.parseInt(values.get(table.getPkColumn().getName())) : pkValue;
		byteBuffer.putInt(newPkValue);
		byteBuffer.put(file.readByte());
		for(Map.Entry<String, Column> entry : columns.entrySet()) {
			String key = entry.getKey();
			Column column = entry.getValue();
			if(!column.isPk()) {
				byte dataType = file.readByte();
				if (!column.isDataTypeCorrect(dataType)) {
					displayError("Datatype is not correct.");
					return new SplitPage(-1);
				}
				String valuesColumn = values.get(column.getName());
				int dataTypeLength = UtilityTools.getNumberOfBytesFromTypebyte(dataType);
				if(valuesColumn != null) {
					byteBuffer.put(column.getColumnValue(valuesColumn).getDataTypeOfValue());
					byteBuffer.put(column.getColumnValue(valuesColumn).getByteValue());
					//Skip the bytes so that the columns are consistent.
					file.skipBytes(dataTypeLength);
				} else {
					byteBuffer.put(dataType);
					byte[] bytes = new byte[dataTypeLength];
					file.read(bytes);
					byteBuffer.put(bytes);
				}
			}
		}
		byte[] newRecord = byteBuffer.array();
		List<Short> cellLocations = pageHeader.getCellLocations();
		if(needToDeleteAndUpdate) {
			//Delete the old record
			DeleteCommand deleteCommand = new DeleteCommand(new DeleteParams(file, 0, table, new Condition(table.getPkColumn(), "=", new IntType(pkValue))));
			deleteCommand.executeTraverseAndDelete();
			//Insert the new record
			InsertCommand insertCommand = new InsertCommand(new InsertParams(file, newPkValue, 0, null, table, newRecord));
			SplitPage insertionSplitPage = insertCommand.executeTraverseAndInsert();
			return new SplitPage(insertionSplitPage.isInserted() ? newPkValue : -1, insertionSplitPage.isInserted());
		} else {
			if(newRecordLength == oldRecordLength) {
				//If record length is the same just rewrite the record with the new one.
				file.seek(pageStartPointer + pageHeader.getCellLocations().get(searchKey));
				file.write(newRecord);
				return new SplitPage(newPkValue, false);
			} else if(newRecordLength < oldRecordLength) {
				//If record length is the less than the old record length add the record at the start after pushing the other records.
				//short newLocation = (short) (pageHeader.getCellLocations().get(searchKey) - newRecordLength + oldRecordLength);
				short searchKeyCellOldLocation = pageHeader.getCellLocations().get(searchKey);
				short searchKeyCellNewLocation = (short) (pageHeader.getCellLocations().get(searchKey) - newRecordLength + oldRecordLength);
				if(searchKeyCellOldLocation > pageHeader.getCellContentStartOffset()) {
					byte[] tempBytes = new byte[searchKeyCellOldLocation - pageHeader.getCellContentStartOffset()];
					file.seek(pageStartPointer + pageHeader.getCellContentStartOffset());
					file.read(tempBytes);
					file.seek(pageStartPointer + pageHeader.getCellContentStartOffset() - newRecordLength + oldRecordLength);
					file.write(tempBytes);
				}
				file.seek(pageStartPointer + searchKeyCellNewLocation);
				file.write(newRecord);
				file.seek(pageStartPointer + 8);
				int index = 0;
				for (Short cellLocation : cellLocations) {
					if(cellLocation <= searchKeyCellOldLocation) {
						file.seek(pageStartPointer + 8 + 2 * index);
						file.writeShort(cellLocation - newRecordLength + oldRecordLength);
					}
					index++;
				}
				file.seek(pageStartPointer + 2);
				file.writeShort(pageHeader.getCellContentStartOffset() - newRecordLength + oldRecordLength);
				return new SplitPage(newPkValue, false);
			} else {
				//Check if enough space is available.
				int availableSpace = pageHeader.getCellContentStartOffset() - pageHeader.getHeaderEndOffset();
				if(newRecordLength - oldRecordLength <= availableSpace) {
					//space is available.
					Short searchKeyCellLocation = pageHeader.getCellLocations().get(searchKey);
					byte[] tempBytes = new byte[searchKeyCellLocation - pageHeader.getCellContentStartOffset()];
					file.seek(pageStartPointer + pageHeader.getCellContentStartOffset());
					file.read(tempBytes);
					file.seek(pageStartPointer + pageHeader.getCellContentStartOffset() + oldRecordLength);
					file.write(tempBytes);
					//Update the new cell locations
					int index = 0;
					for (Short cellLocation : cellLocations) {
						if(cellLocation < searchKeyCellLocation) {
							file.seek(pageStartPointer + 8 + 2 * index);
							file.writeShort(cellLocation + oldRecordLength);
						}
						index++;
					}
					file.seek(pageStartPointer + pageHeader.getCellContentStartOffset() + oldRecordLength - newRecordLength);
					file.write(newRecord);
					//Update the search key location
					file.seek(pageStartPointer + 8 + 2 * searchKey);
					file.writeShort(pageHeader.getCellContentStartOffset() + oldRecordLength - newRecordLength);
					file.seek(pageStartPointer + 2);
					file.writeShort(pageHeader.getCellContentStartOffset() + oldRecordLength - newRecordLength);
					return new SplitPage(newPkValue, false);
				} else {
					//Space is not available. Split the page.
					//Delete and insert the record.
					DeleteCommand deleteCommand = new DeleteCommand(new DeleteParams(file, 0, table, new Condition(table.getPkColumn(), "=", new IntType(pkValue))));
					deleteCommand.executeTraverseAndDelete();
					InsertCommand insertCommand = new InsertCommand(new InsertParams(file, newPkValue, 0, null, table, newRecord));
					SplitPage insertionSplitPage = insertCommand.executeTraverseAndInsert();
					return new SplitPage(insertionSplitPage.isInserted() ? newPkValue : -1, insertionSplitPage.isInserted());
				}
			}
		}
	}


}
