package lodVader.mongodb.collections.namespaces;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.DBSuperClass;

public class DistributionObjectNS0DB extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "DistributionObjectNS0";

	
	// class properties
	public static final String DISTRIBUTION_ID = "distributionID";
	
	public static final String OBJECT_NS0 = "objectNS0";
	
	private int distributionID;

	private String objectNS0;

	
	public DistributionObjectNS0DB(String uri) {
		
		super(COLLECTION_NAME, uri);
		loadObject();
	}

	public boolean updateObject(boolean checkBeforeInsert) throws LODVaderLODGeneralException {

		BasicDBObject mongoDBObject2 = new BasicDBObject();
		
		// save object case it doens't exists
		try {
			// updating subjectsTarget on mongodb
			mongoDBObject.put(DISTRIBUTION_ID, distributionID);
			mongoDBObject2.put(DISTRIBUTION_ID, distributionID);
			
			mongoDBObject.put(OBJECT_NS0, objectNS0);
			mongoDBObject2.put(OBJECT_NS0, objectNS0);

			DBCursor d = objectCollection.find(mongoDBObject2);
			if (d.hasNext())
				return false;

			
			insert(checkBeforeInsert);
		} catch (Exception e2) {
			// e2.printStackTrace();

			try {
				if (update())
					return true;
				else
					return false;
			} catch (LODVaderLODGeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		}
		return false;
	}

	protected boolean loadObject() {
		DBObject obj = search();

		if (obj != null) {

			distributionID = ((Number) obj.get(DISTRIBUTION_ID)).intValue();
			objectNS0 = obj.get(OBJECT_NS0).toString();
			
			return true;
		}
		return false;
	}

	public int getDistributionID() {
		return distributionID;
	}

	public void setDistributionID(int distributionID) {
		this.distributionID = distributionID;
	}

	public String getObjectNS0() {
		return objectNS0;
	}

	public void setObjectNS0(String objectNS0) {
		this.objectNS0 = objectNS0;
	}

	

}
