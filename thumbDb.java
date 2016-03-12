package thumbDb;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import thumbDb.DatabaseHelper;

public class thumbDb {
	public static void handleCommands(String line, DataBase db){	
		String[] operands = line.split("\\s+");
		String cmd = operands[0];
		try {
			switch (cmd) {
			case "SET":
				String key = operands[1];
				Integer value = Integer.parseInt(operands[2]);
				db.set(key, value);
				break;
			case "GET":
				String name = operands[1];
				System.out.println(db.get(name) != null ? db.get(name):"NULL");
				break;
			case "UNSET":
				name = operands[1];
				db.set(name, null);
				break;
			case "NUMEQUALTO":
				value = Integer.parseInt(operands[1]);
				System.out.println(db.numEqTo(value));
				break;
			case "BEGIN":
				db.beginTransaction();
				break;
			case "COMMIT":
				if (!db.commitTransaction()){
					System.out.println("NO TRANSACTION");
				}
				break;	
			case "ROLLBACK":
				if (!db.rollBackTransaction()){
					System.out.println("NO TRANSACTION");
				}
				break;
			case "END":
				System.exit(0);
			default:
				System.out.println("Invalid command: " + cmd );
			}
		} catch (NumberFormatException e) {
			System.out.println("Invalid number format: " + line );
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Possibly missing operand: " + line);
		}
	}

	public static void main(String[] args) throws IOException {
		DataBase db = new DatabaseHelper().getDatabase();
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(System.in));
			String line;
			while ((line = in.readLine()) != null) {
				handleCommands(line, db);
			}
		}
		catch (IOException e) {
			System.out.println("Exception when reading from stdin");
			throw e;
		}
		finally {
			if (in != null) {
				in.close();
			}
		}

	}
}
