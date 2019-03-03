package lodVader.mongodb.collections;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;


public class LODVaderCounterDBBackup {
	
	
	DBSuperClass2 db;

	// Collection name
	public static final String COLLECTION_NAME = "LODVaderCounter";

	public static final String COUNTER_VALUE = "counterValue";

	public static final String COUNTER_NAME = "counterName";

	public LODVaderCounterDBBackup(DBSuperClass2 db) {
		this.db = db;
		this.db.addPK(COUNTER_NAME);
		setCounterName("LodVaderCounter");
		this.db.find(true);
	}
	
	public int getCounterValue() { 
		return Integer.parseInt(db.getField(COUNTER_VALUE).toString());
	}

	public void setCounterValue(int counter) {
		db.addField(COUNTER_VALUE, counter);
	}
	
	public String getCounterName() { 
		return db.getField(COUNTER_NAME).toString();
	}

	public void setCounterName(String counter) {
		db.addField(COUNTER_NAME, counter);
	}

	public synchronized int incrementAndGetID(){
		db.find(true);
		setCounterValue(getCounterValue()+1);
		try {
			db.update(true);
		} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException
				| LODVaderNoPKFoundException e) {
			e.printStackTrace();
		}
		return getCounterValue();
	}
	

}
