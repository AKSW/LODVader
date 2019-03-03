package lodVader.mongodb.collections.namespaces;

import java.util.ArrayList;
import java.util.Set;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DistributionDB;

public class SuperNS{
	
	DBSuperClass2 db;
	
	public SuperNS(DBSuperClass2 db) {
		this.db = db;
	}

	public void init(String collectionName) {
		this.db.COLLECTION_NAME = collectionName;
		setParameters();
	}
	
	private void setParameters(){
		db.addMandatoryField(DATASET_ID);
		db.addMandatoryField(DISTRIBUTION_ID);
		db.addMandatoryField(NS);
	}
	
	// class properties
	public static final String DISTRIBUTION_ID = "distributionID";

	public static final String DATASET_ID = "datasetID";

	public static final String NS = "ns";

	public int getDistributionID() {
		return ((Number) db.mongoDBObject.get(DISTRIBUTION_ID)).intValue();
	}

	public void setDistributionID(int distributionID) {
		db.mongoDBObject.put(DISTRIBUTION_ID, distributionID);
	}

	public int getDatasetID() {
		return ((Number) db.mongoDBObject.get(DATASET_ID)).intValue();
	}

	public void setDatasetID(int datasetID) {
		db.mongoDBObject.put(DATASET_ID, datasetID);
	}

	public String getNS() {
		return db.getField(NS).toString();
	}

	public void setNS(String ns) {
		db.addField(NS, ns);
	}
	
	public void bulkSave(Set<String> nsSet, DistributionDB distribution){
		db.getCollection().remove(new BasicDBObject(DISTRIBUTION_ID, distribution.getLODVaderID()));
		int distributionLodVaderID = distribution.getLODVaderID();
		int topDatasetID = distribution.getTopDatasetID();
		ArrayList<DBObject> bulkList = new ArrayList<DBObject>();
		
		for (String s : nsSet) {
			db.mongoDBObject = new BasicDBObject();
			setDistributionID(distributionLodVaderID);
			setNS(s);
			setDatasetID(topDatasetID);
			bulkList.add(db.mongoDBObject);
		}
		
		db.bulkSave2(bulkList);
		
	}

}
