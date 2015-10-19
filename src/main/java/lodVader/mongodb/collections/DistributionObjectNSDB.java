package lodVader.mongodb.collections;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.DBSuperClass;

public class DistributionObjectNSDB extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "DistributionObjectNS";

	
	// class properties
	public static final String DISTRIBUTION_ID = "distributionID";
	
	public static final String OBJECT_NS = "objectNS";	
	
	public static final String NUMBER_OF_RESOURCES = "numberOfResources";	
	
	
	private int distributionID;

	private String objectNS;

	private int numberOfResources;

	
	
	public DistributionObjectNSDB(String uri) {
		
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

			// updating objectsTarget on mongodb
			mongoDBObject.put(OBJECT_NS, objectNS);
			mongoDBObject2.put(OBJECT_NS, objectNS);
			
			// updating number of resources on mongodb
			mongoDBObject.put(NUMBER_OF_RESOURCES, numberOfResources);
			mongoDBObject2.put(NUMBER_OF_RESOURCES, numberOfResources);

			
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

			objectNS = (String) obj.get(OBJECT_NS);

			numberOfResources = ((Number) obj.get(NUMBER_OF_RESOURCES)).intValue();


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
 
	public String getObjectFQDN() {
		return objectNS;
	}

	public void setObjectFQDN(String objectFqdn) {
		this.objectNS = objectFqdn;
	}

	public int getNumberOfResources() {
		return numberOfResources;
	}

	public void setNumberOfResources(int numberOfResources) {
		this.numberOfResources = numberOfResources;
	}
	
	

}
