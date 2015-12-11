package lodVader.mongodb.collections;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;

public class LODVaderCounterDB extends DBSuperClass2 {

	// Collection name
	public static final String COLLECTION_NAME = "LODVaderCounter";

	public static final String COUNTER_VALUE = "counterValue";

	public static final String COUNTER_NAME = "counterName";

	public LODVaderCounterDB() {
		super(COLLECTION_NAME);
		addPK(COUNTER_NAME);
		setCounterName("LodVaderCounter");
		find(true);
	}
	
	public int getCounterValue() { 
		return Integer.parseInt(getField(COUNTER_VALUE).toString());
	}

	public void setCounterValue(int counter) {
		addField(COUNTER_VALUE, counter);
	}
	
	public String getCounterName() { 
		return getField(COUNTER_NAME).toString();
	}

	public void setCounterName(String counter) {
		addField(COUNTER_NAME, counter);
	}

	public synchronized int incrementAndGetID(){
		find(true);
		setCounterValue(getCounterValue()+1);
		try {
			update(true);
		} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException
				| LODVaderNoPKFoundException e) {
			e.printStackTrace();
		}
		return getCounterValue();
	}

}
