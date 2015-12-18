package lodVader.mongodb.collections.RDFResources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass2;

public class GeneralRDFResourceRelationDB extends DBSuperClass2{
	
	public static final String PREDICATE_ID = "predicateID";

	public static final String DATASET_ID = "datasetID";

	public static final String DISTRIBUTION_ID = "distributionID";
	
	public static final String AMOUNT = "amount";
	
	public static final String ID = "id";
	
	public static String COLLECTION_NAME;

	
	public GeneralRDFResourceRelationDB(String collection, DBObject obj) {
		super(collection,obj);
		setParameters();
		COLLECTION_NAME = collection;
	}
	
	public GeneralRDFResourceRelationDB(String collection) { 
		super(collection);
		setParameters();
		COLLECTION_NAME = collection;
	}
	
	
	public GeneralRDFResourceRelationDB(String collection, String id) {
		super(collection);
		COLLECTION_NAME = collection;
		setParameters();
		setId(id);
		find(true);
	}
	
	private void setParameters(){
		addPK(ID);
		addMandatoryField(DATASET_ID);
		addMandatoryField(DISTRIBUTION_ID);
		addMandatoryField(AMOUNT);
	}

	public String getId() {
		return getField(ID).toString();
	}
	
	public int getPredicateID() {
		return Integer.parseInt(getField(PREDICATE_ID).toString());
	}

	public void setPredicateID(int predicateID) { 
		addField(PREDICATE_ID, predicateID);
	}
	
	public void setId(String id) {
		addField(ID, id);
	}

	public int getDatasetID() {
		return Integer.parseInt(getField(DATASET_ID).toString());
}

	public void setDatasetID(int datasetID) {
		addField(DATASET_ID, datasetID);
	}

	public int getDistributionID() {
		return Integer.parseInt(getField(DISTRIBUTION_ID).toString());
	}

	public void setDistributionID(int distributionID) {
		addField(DISTRIBUTION_ID, distributionID);
	}
	
	
	public int getAmount() {
		return Integer.parseInt(getField(AMOUNT).toString());
	}

	public void setAmount(int amount) {
		addField(AMOUNT, amount);
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

			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(
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
				GeneralRDFResourceRelationDB r = new GeneralRDFResourceRelationDB(COLLECTION_NAME, instance);
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
			
			
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(
					COLLECTION_NAME);

			DBCursor cursor = collection.find(query);

			// save a list with distribution and fqdn 
			while (cursor.hasNext()) {
				DBObject instance = cursor.next();
				GeneralRDFResourceRelationDB r = new GeneralRDFResourceRelationDB(COLLECTION_NAME, instance);
				result.add(r);
			} 

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return result;
		
	}
	
}
