package lodVader.mongodb.collections;

import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.DBSuperClass;

public class LODVaderCounterDB extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "LODVaderCounter";

	public static final String COUNTER = "counter";

	// class properties

	
	private Integer counter = 0;
	

	public LODVaderCounterDB() {
		super(COLLECTION_NAME, COLLECTION_NAME);
		loadObject();
	}

	public boolean updateObject(boolean checkBeforeInsert) {
		return false;
	}
	private boolean updateObject() {
		try {

			mongoDBObject.put(COUNTER, counter);

			insert(true);
			return true;
		} catch (Exception e2) {
			// e2.printStackTrace();

			try {
				if (update())
					return true;
				else
					return false;
			} catch (LODVaderLODGeneralException e) {
				e.printStackTrace();
				return false;
			}
		}
	}

	protected boolean loadObject() {
		DBObject obj = search();

		if (obj != null) {
			
			counter = (Integer) obj.get(COUNTER);
			
			return true;
		}
		return false;
	}

	
	public synchronized int incrementAndGetID(){
		loadObject(); 
		counter++;
		updateObject();
		return counter.intValue(); 
	}

}
