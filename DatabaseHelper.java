package thumbDb;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map.Entry;

import thumbDb.Transaction;

class DataBase{
	private LinkedList<Transaction> transactions;

	public DataBase(){
		this.transactions = new LinkedList<Transaction>();
		this.addTransaction(new Transaction());
	}

	private void clearAllTransactions(){
		while (!this.transactions.isEmpty()) {
			this.transactions.removeFirst();
		}
	}

	private void addTransaction(Transaction tr){
		this.transactions.add(tr);
	}

	private void deleteLastTransaction(){
		this.transactions.removeLast();
	}

	private Transaction getLastTransaction(){
		return this.transactions.getLast();
	}

	public void set(String keyname, Integer value){
		this.getLastTransaction().set(keyname, value);
	}

	public Integer get(String keyname){
		return this.getLastTransaction().get(keyname);
	}

	public Integer numEqTo(Integer value){
		return this.getLastTransaction().numEqTo(value);
	}

	private Transaction mergeTransactions(){
		// A dictionary to keep track of keys and values in a Transaction
		HashMap<String, Integer> keyval = new HashMap<String, Integer>();
		// A dictionary to keep track of count of values in a transactions
		HashMap<Integer, Integer> valuecount = new HashMap<Integer, Integer>();

		ListIterator<Transaction> iterator = this.transactions.listIterator();
		while (iterator.hasNext()) {
			Transaction tr = iterator.next();
			keyval.putAll(tr.getCurrentState()) ;
		}

		for (Entry<String, Integer> entry : keyval.entrySet()) {
			Integer value = entry.getValue();
			if(valuecount.get(value) == null){
				valuecount.put(value, new Integer(1));
			}
			else{
				valuecount.put(value, new Integer(valuecount.get(value) + 1));
			}
			keyval.put(entry.getKey(), entry.getValue());
		}

		return new Transaction(keyval, valuecount);
	}

	public boolean commitTransaction(){
		if (this.transactions.size() <= 1){
			return false;
		}
		// Merge all existing Transactions
		Transaction mergedTransaction = mergeTransactions();
		// Clear existing Transactions in the database
		this.clearAllTransactions();
		// Add the new merged Transaction
		this.transactions = new LinkedList<Transaction>();
		this.transactions.add(mergedTransaction);
		return true;
	}

	public boolean rollBackTransaction(){
		if (this.transactions.size() <= 1) {
			return false;
		}

		this.deleteLastTransaction();
		return true;
	}

	public void beginTransaction(){
		Transaction trans = new Transaction();
		trans.setPreviousState(this.getLastTransaction());
		this.addTransaction(trans);
	}
}


public class DatabaseHelper {
	private DataBase db;
	
	public DatabaseHelper(){
		if (this.db == null){
			this.db = new DataBase();
		}
	}

	public DataBase getDatabase(){
		return this.db;
	}
}
