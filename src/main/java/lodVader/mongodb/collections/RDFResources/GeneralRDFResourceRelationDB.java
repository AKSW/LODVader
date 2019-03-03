package lodVader.mongodb.collections.RDFResources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.configuration.Config;
import lodVader.mongodb.DBSuperClass2;

public class GeneralRDFResourceRelationDB{
	
	@Autowired
	Config conf;
	
	public DBSuperClass2 db;
	
	public static final String PREDICATE_ID = "predicateID";

	public static final String DATASET_ID = "datasetID";

	public static final String DISTRIBUTION_ID = "distributionID";
	
	public static final String AMOUNT = "amount";
	
	public static final String ID = "id";
	
	public static String COLLECTION_NAME;

	
	public GeneralRDFResourceRelationDB(DBSuperClass2 db){
		this.db = db;
	}
	
	public void init(String collection, DBObject obj) {
		db.COLLECTION_NAME = collection;
		db.mongoDBObject = obj;
		setParameters();
	}
	
	public void init(String collection) { 
		db.COLLECTION_NAME = collection;
		setParameters();
	}
	
	
	public void init(String collection, String id) {
		db.COLLECTION_NAME = collection;
		setParameters();
		setId(id);
		db.find(true);
	}
	
	private void setParameters(){
		db.addPK(ID);
		db.addMandatoryField(DATASET_ID);
		db.addMandatoryField(DISTRIBUTION_ID);
		db.addMandatoryField(AMOUNT);
	}

	public String getId() {
		return db.getField(ID).toString();
	}
	
	public int getPredicateID() {
		return Integer.parseInt(db.getField(PREDICATE_ID).toString());
	}

	public void setPredicateID(int predicateID) { 
		db.addField(PREDICATE_ID, predicateID);
	}
	
	public void setId(String id) {
		db.addField(ID, id);
	}

	public int getDatasetID() {
		return Integer.parseInt(db.getField(DATASET_ID).toString());
}

	public void setDatasetID(int datasetID) {
		db.addField(DATASET_ID, datasetID);
	}

	public int getDistributionID() {
		return Integer.parseInt(db.getField(DISTRIBUTION_ID).toString());
	}

	public void setDistributionID(int distributionID) {
		db.addField(DISTRIBUTION_ID, distributionID);
	}
	
	
	public int getAmount() {
		return Integer.parseInt(db.getField(AMOUNT).toString());
	}

	public void setAmount(int amount) {
		db.addField(AMOUNT, amount);
	}

	/**
	 * Store a set of rdf:type values
	 * @param set
	 */
	public void insertSet(HashMap<String, Integer> set, int distributionID, int datasetID){}
	
	public Set<String> getSetOfPredicates(int distributionDynLODID){return null;}
	
	public List<GeneralRDFResourceRelationDB> getTopNPredicates(int distributionID, int topN){
		List<GeneralRDFResourceRelationDB> result = new ArrayList<GeneralRDFResourceRelationDB>();
		
		try {

			DBCollection collection = db.getCollection(
					COLLECTION_NAME);

			// query all fqdn
			BasicDBObject query = new BasicDBObject(
					GeneralRDFResourceRelationDB.DISTRIBUTION_ID,
					distributionID);
			BasicDBObject sort = new BasicDBObject(GeneralRDFResourceRelationDB.AMOUNT, -1);

			DBCursor cursor = collection.find(query).sort(sort).limit(topN);

			// save a list with distribution and fqdn
			while (cursor.hasNext()) {
				DBObject instance = cursor.next();
				GeneralRDFResourceRelationDB r = conf.getGeneralRDFResourceRelationDB();
				result.add(r);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public Set<GeneralRDFResourceRelationDB> getPredicatesIn(Set<Integer> in, int distribution1, int distribution2){
		
		HashSet<GeneralRDFResourceRelationDB> result = new HashSet<GeneralRDFResourceRelationDB>();
		
		try {

			// query all fqdn
			BasicDBObject queryIn = new BasicDBObject(
					GeneralRDFResourceRelationDB.PREDICATE_ID,
					new BasicDBObject("$in", in));
			
			BasicDBObject or1 = new BasicDBObject(
					GeneralRDFResourceRelationDB.DISTRIBUTION_ID,
					distribution1);
			BasicDBObject or2 = new BasicDBObject(
					GeneralRDFResourceRelationDB.DISTRIBUTION_ID,
					distribution2);

			BasicDBList or = new BasicDBList();
			or.add(or1);
			or.add(or2);
			BasicDBObject queryOr = new BasicDBObject("$or", or);
			
			BasicDBList and = new BasicDBList();
			and.add(queryIn);
			and.add(queryOr);
			BasicDBObject query = new BasicDBObject("$and", and);
			
			
			DBCollection collection = db.getCollection(
					COLLECTION_NAME);

			DBCursor cursor = collection.find(query);

			// save a list with distribution and fqdn 
			while (cursor.hasNext()) {
				DBObject instance = cursor.next();
				
				GeneralRDFResourceRelationDB r = conf.getGeneralRDFResourceRelationDB();
				r.init(COLLECTION_NAME, instance);
				result.add(r);
			} 

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return result;
		
	}
	
}
