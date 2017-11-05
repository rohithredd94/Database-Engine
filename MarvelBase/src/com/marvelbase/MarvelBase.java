package com.marvelbase;

import com.marvelbase.Commands.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.marvelbase.UtilityTools.sbTablesTable;


public class MarvelBase {

	private static boolean isExit = false;
	private static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	private static String currentDb = null;

	public static void main(String[] args) {
		response(line("-", 80));
		response("Welcome to MarvelBase");
		response("\nType \"help;\" to display supported commands.");
		response(line("-", 80));
		initDatabase();
		String commandInput;
		while(!isExit) {
			System.out.print("marvelbase> ");
			commandInput = scanner.next().replace("\n", " ").replace("\r", " ").trim().toLowerCase();
			Pattern multi_spaces_pattern = Pattern.compile("\\s+(?=(?:[^\\'\"]*[\\'\"][^\\'\"]*[\\'\"])*[^\\'\"]*$)");
 			Matcher m = multi_spaces_pattern.matcher(commandInput);
 			commandInput = m.replaceAll(" ");
			parseCommandInput(commandInput);
		}
		response("Exiting...");
	}

	private static void help() {
		response(line("*", 80));
		response("SUPPORTED COMMANDS");
		response("All commands below are case insensitive");
		System.out.println();
		response("\tSELECT * FROM table_name;                        Display all records in the table.");
		response("\tSELECT * FROM table_name WHERE rowid = <value>;  Display records whose rowid is <id>.");
		response("\tDROP TABLE table_name;                           Remove table data and its schema.");
		response("\tVERSION;                                         Show the program version.");
		response("\tHELP;                                            Show this help information");
		response("\tEXIT;                                            Exit the program");
		System.out.println();
		System.out.println();
		response(line("*", 80));
	}

	public static void parseCommandInput(String commandInput) {
		ArrayList<String> commandTokens = new ArrayList<>(Arrays.asList(commandInput.split(" ")));

		switch (commandTokens.get(0)) {
			case "select":
				SelectCommand selectCommand = new SelectCommand(commandInput, currentDb);
				selectCommand.execute();
				break;
			case "show":
				ShowCommand showCommand = new ShowCommand(commandTokens.get(1), currentDb);
				showCommand.execute();
				break;
			case "drop":
				if (commandTokens.get(1).equals("database")) {
					DropDatabaseCommand dropDatabaseCommand = new DropDatabaseCommand(commandInput);
					if(dropDatabaseCommand.execute()) {
						if(currentDb.equals(dropDatabaseCommand.getDeletedDb()))
							currentDb = null;
					}
				} else if (commandTokens.get(1).equals("table")) {
					DropTableCommand dropTableCommand = new DropTableCommand(commandInput, currentDb);
					dropTableCommand.execute();
				} else {
					logMessage("Wrong type of drop called");
					return;
				}
				break;
			case "create":
				if (commandTokens.get(1).equals("database")) {
					CreateDatabaseCommand createDatabaseCommand = new CreateDatabaseCommand(commandInput);
					createDatabaseCommand.execute();
				} else if (commandTokens.get(1).equals("table")) {
					CreateTableCommand createTableCommand = new CreateTableCommand(commandInput, currentDb);
					createTableCommand.execute();
				} else {
					logMessage("Wrong type of create called");
					return;
				}
				break;
			case "delete":
				DeleteCommand deleteCommand = new DeleteCommand(commandInput, currentDb);
				deleteCommand.execute();
				break;
			case "update":
				UpdateCommand updateCommand = new UpdateCommand(commandInput, currentDb);
				updateCommand.execute();
				break;
			case "insert":
				InsertCommand insertCommand = new InsertCommand(commandInput, currentDb);
				insertCommand.execute();
				break;
			case "use":
				UseCommand useCommand = new UseCommand(commandInput);
				if(useCommand.execute())
					currentDb = useCommand.getCurrentDb();
				break;
			case "help":
				help();
				break;
			case "exit":
				isExit = true;
				break;
			case "quit":
				isExit = true;
				break;
			default:
				displayError("Please check the command entered\nCommand: \"" + commandInput + "\"");
				break;
		}
	}

	public static void response(String response) {
		System.out.println(response);
	}

	public static void displayError(String message) {
		System.out.println(message);
	}

	//Uncomment the System.out to enable logs.
	public static void logMessage(String message) {
		//System.out.println(message);
	}
	
	private static String line(String s, int num) {
		StringBuilder a = new StringBuilder();
		for (int i = 0; i < num; i++) {
			a.append(s);
		}
		return a.toString();
	}
	
	private static void initDatabase() {
		int pkValue = 1;
		File directory = new File("data/catalog");
		if (!directory.exists())
			directory.mkdirs();
		try {
			RandomAccessFile sBTablesTableFile = new RandomAccessFile("data/catalog/mb_tables.tbl", "rw");
			Table sBTablesTable = sbTablesTable;
			if(sBTablesTableFile.length() == 0) {
				logMessage("Tables table inserting");
				sBTablesTableFile.setLength(sBTablesTableFile.length() + UtilityTools.pageSize);
				long initialPagePointer = sBTablesTableFile.getFilePointer();
				sBTablesTableFile.seek(initialPagePointer);
				sBTablesTableFile.writeByte(0x0D);
				sBTablesTableFile.skipBytes(1);
				sBTablesTableFile.writeShort((int) UtilityTools.pageSize);
				sBTablesTableFile.writeInt(-1);
				//Tables table
				LinkedHashMap<String, String> firstValues = new LinkedHashMap<>(), secondValues = new LinkedHashMap<>();
				firstValues.put("row_id", pkValue + "");
				firstValues.put("table_name", "mb_tables");
				firstValues.put("database_name", "catalog");
				firstValues.put("record_count", "2");
				firstValues.put("avg_length", "0");
				int recordsLength = sBTablesTable.getRecordLength(firstValues);
				pkValue++;
				secondValues.put("row_id", pkValue + "");
				secondValues.put("table_name", "mb_columns");
				secondValues.put("database_name", "catalog");
				secondValues.put("record_count", "11");
				secondValues.put("avg_length", "0");
				recordsLength += sBTablesTable.getRecordLength(secondValues);
				firstValues.put("avg_length", (recordsLength / 2) + "");
				if (!sBTablesTable.validateValues(firstValues)) {
					logMessage("First values not valid");
					return;
				}
				InsertCommand insertCommand = new InsertCommand(new InsertParams(sBTablesTableFile, Integer.parseInt(firstValues.get("row_id")), 0, firstValues, sBTablesTable, null));
				insertCommand.executeTraverseAndInsert();
				if (!sBTablesTable.validateValues(secondValues)) {
					logMessage("Second values not valid");
					return;
				}
				insertCommand = new InsertCommand(new InsertParams(sBTablesTableFile, Integer.parseInt(secondValues.get("row_id")), 0, secondValues, sBTablesTable, null));
				insertCommand.executeTraverseAndInsert();
			}
			sBTablesTableFile.close();
			RandomAccessFile sBColumnsTableFile = new RandomAccessFile("data/catalog/mb_columns.tbl", "rw");
			Table sbColumnsTable = UtilityTools.sbColumnsTable;
			if(sBColumnsTableFile.length() == 0) {
				logMessage("Columns table inserting");
				sBColumnsTableFile.setLength(sBColumnsTableFile.length() + UtilityTools.pageSize);
				long initialPagePointer = sBColumnsTableFile.getFilePointer();
				sBColumnsTableFile.seek(initialPagePointer);
				sBColumnsTableFile.writeByte(0x0D);
				sBColumnsTableFile.skipBytes(1);
				sBColumnsTableFile.writeShort((int) UtilityTools.pageSize);
				sBColumnsTableFile.writeInt(-1);
				for (LinkedHashMap<String, String> values : UtilityTools.getSbColumnsTableValues()) {
					if (!sbColumnsTable.validateValues(values)) {
						logMessage("First values not valid");
						return;
					}
					InsertCommand insertCommand = new InsertCommand(new InsertParams(sBColumnsTableFile, Integer.parseInt(values.get("row_id")), 0, values, sbColumnsTable, null));
					insertCommand.executeTraverseAndInsert();
				}
			}
			sBColumnsTableFile.close();
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		}
	}
}