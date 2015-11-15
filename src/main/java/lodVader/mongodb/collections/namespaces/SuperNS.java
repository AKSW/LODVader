package lodVader.mongodb.collections.namespaces;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.DBSuperClass;

public class SuperNS {

	// Collection name
	public static String COLLECTION_NAME = null;

	// class properties
	public static final String DISTRIBUTION_ID = "distributionID";

	public static final String DATASET_ID = "datasetID";

	public static final String NS = "ns";

	public BasicDBObject mongoDBObject = new BasicDBObject();

	private DBCollection collection = null;

	public boolean updateObject(boolean checkBeforeInsert) throws LODVaderMissingPropertiesException {
		
		if (checkBeforeInsert) {
			DBCursor d = getCollection().find(mongoDBObject);
			if (d.hasNext())
				return false;
		}
		mongoDBObject.put("_id", new ObjectId().get().toString());
		if (checkField(DISTRIBUTION_ID) && checkField(DATASET_ID) && checkField(NS))
			getCollection().insert(mongoDBObject);
		return true;
	
	}

	public void removeObject() throws LODVaderMissingPropertiesException {
		if (checkField(DISTRIBUTION_ID) && checkField(DATASET_ID) && checkField(NS))
			getCollection().remove(mongoDBObject);
	}

	public int getDistributionID() {
		return ((Number) mongoDBObject.get(DISTRIBUTION_ID)).intValue();
	}

	public void setDistributionID(int distributionID) {
		mongoDBObject.put(DISTRIBUTION_ID, distributionID);
	}

	public int getDatasetID() {
		return ((Number) mongoDBObject.get(DATASET_ID)).intValue();
	}

	public void setDatasetID(int datasetID) {
		mongoDBObject.put(DATASET_ID, datasetID);
	}

	public String getNS() {
		return mongoDBObject.get(NS).toString();
	}

	public void setNS(String ns) {
		mongoDBObject.put(NS, ns);
	}

	protected void addField(String key, String val) {
		mongoDBObject.put(key, val);
	}

	protected void addField(String key, int val) {
		mongoDBObject.put(key, val);
	}

	protected Object getField(String key) {
		return mongoDBObject.get(key);
	}

	public DBCollection getCollection() {
		if (collection == null)
			collection = DBSuperClass.getInstance().getCollection(COLLECTION_NAME);
		return collection;
	}

	private boolean checkField(Object key) throws LODVaderMissingPropertiesException {
		if (!mongoDBObject.containsKey(key))
			throw new LODVaderMissingPropertiesException("Missing property: " + key.toString());
		return true;
	}

}
