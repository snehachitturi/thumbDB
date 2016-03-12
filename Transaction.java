package thumbDb;

import java.util.HashMap;

class Transaction {
	private Transaction prevTransaction;

	private HashMap<String, Integer> keyval;
	private HashMap<Integer, Integer> valuecount;

	public Transaction(){
		this.prevTransaction = null;
		this.keyval = new HashMap<String, Integer>();
		this.valuecount = new HashMap<Integer, Integer>();
	}

	public Transaction(HashMap<String, Integer> kv, HashMap<Integer, Integer> vc){
		this.keyval = kv;
		this.valuecount = vc;
	}

	public HashMap<String, Integer> getCurrentState(){
		return this.keyval;
	}

	public void setPreviousState(Transaction tr) {
		this.prevTransaction = tr;
	}

	public Integer numEqTo(Integer value){
		Transaction tr = this;
		Integer currentValCnt = tr.valuecount.get(value);

		// Go back through all existing Transactions to get the counter for this value
		while(currentValCnt == null && tr.prevTransaction != null){
			tr = tr.prevTransaction;
			currentValCnt = tr.valuecount.get(value);
		}

		if (currentValCnt == null){
			return 0;
		}
		else{
			return currentValCnt;
		}
	}


	public void set(String name, Integer currentValue){
		// If an existing key is set to a new value, we need to reduce the counter
		// for that value
		Integer prevValue = get(name);
		if (prevValue != null){
			Integer prevValueCounter = numEqTo(prevValue);
			this.valuecount.put(prevValue, --prevValueCounter);
		}

		// Bump up the counter for this value in the value counter or add a new one
		Integer currentValueCounter = numEqTo(currentValue);
		if (currentValue != null) {
			if (currentValueCounter != null) {
				this.valuecount.put(currentValue, ++currentValueCounter);
			} else {
				this.valuecount.put(currentValue, new Integer(1));
			}
		}

		this.keyval.put(name, currentValue);
	}

	// Get the value for the given name
	public Integer get(String name) {
		Transaction tr = this;
		Integer value = tr.keyval.get(name);

		// Go back in transactions to get value for this key
		while(!tr.keyval.containsKey(name) && tr.prevTransaction != null){
			tr = tr.prevTransaction;
			value = tr.keyval.get(name);
		}

		return value;
	}
}
