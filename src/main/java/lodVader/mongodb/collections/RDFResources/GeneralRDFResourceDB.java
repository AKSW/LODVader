package lodVader.mongodb.collections.RDFResources;

import java.util.Set;

import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.LODVaderCounterDB;

public abstract class GeneralRDFResourceDB extends DBSuperClass  {

	public GeneralRDFResourceDB(String collectionName, int lodVaderID) {
		super(collectionName, lodVaderID);
		this.lodVaderID = lodVaderID;
		mongoDBObject.put(LOD_VADER_ID, lodVaderID);
		loadObject();
	}

	public GeneralRDFResourceDB(String collectionName, String id) {
		super(collectionName, id);
		loadObject();
	}
	
	public GeneralRDFResourceDB() {
		super();
	}

	public final String LOD_VADER_ID = "lodVaderID";

	protected int lodVaderID = 0;
	

	public int getLodVaderID() {
		return lodVaderID;
	}

	public void setLODVaderID(int lodVaderID) {
		this.lodVaderID = lodVaderID;
	}
	
	@Override
	public boolean updateObject(boolean checkBeforeInsert)
			throws LODVaderLODGeneralException {
		// save object case it doens't exists
		try {
			if (lodVaderID == 0)
				lodVaderID = new LODVaderCounterDB()
						.incrementAndGetID();
			mongoDBObject.put(LOD_VADER_ID, lodVaderID);

			updateLocalVariables();
			
			insert(checkBeforeInsert);
			return true;

		} catch (Exception e2) {
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

	@Override
	protected boolean loadObject() {
		DBObject obj = search();
		if (obj != null) {
			// mongoDBObject = (BasicDBObject) obj;
			uri = (String) obj.get(URI);
			lodVaderID = (Integer) obj.get(LOD_VADER_ID);
			if (lodVaderID == 0)
				lodVaderID = new LODVaderCounterDB()
						.incrementAndGetID();
			loadLocalVariables();
			return true;
		}
		else
			return false;
	}	

	abstract public void loadLocalVariables();
	
	abstract public void updateLocalVariables();
	
	abstract public void insertSet(Set<String> set);
	

	
}
