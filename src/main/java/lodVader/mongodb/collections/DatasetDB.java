package lodVader.mongodb.collections;

import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.queries.DatasetQueries;

public class DatasetDB extends ResourceDB {
	
	@Autowired
	LODVaderCounterDB lodVaderCounterDB;

	// Collection name
	public static final String COLLECTION_NAME = "Dataset";

	public static final String PARENT_DATASETS = "parentDatasets";

	public static final String SUBSET_IDS = "subsetIDs";

	public static final String DISTRIBUTIONS_IDS = "distributionsIDs";

	public static final String OBJECT_FILENAME = "objectFileName";

	public static final String SUBJECT_FILTER_FILENAME = "subjectFileName";

	public static final String DESCRIPTION_FILE_URL = "descriptionFileURL";

	public DatasetDB(DBSuperClass2 db) {
		super(db, COLLECTION_NAME);
	}
	
	public void init(String uri) {
		setKeys();
		setUri(uri);
		db.find(true);
		if (getLODVaderID() == null)
			setLodVaderID(lodVaderCounterDB.incrementAndGetID());		
	}

	public void init(int id) {
		setKeys();
		setLodVaderID(id);
		db.find(true);
		if (getLODVaderID() == null)
			setLodVaderID(lodVaderCounterDB.incrementAndGetID());
	}

	public void init(DBObject object) {
		setKeys();
		db.mongoDBObject = object;
	}

	public void setKeys() {
		db.addPK(URI);
		db.addPK(LOD_VADER_ID);
		db.addMandatoryField(URI);
		db.addMandatoryField(DESCRIPTION_FILE_URL);
//		db.addMandatoryField(SUBSET_IDS);
//		db.addMandatoryField(DISTRIBUTIONS_IDS);
	}

	public void setSubsetIds(ArrayList<Integer> ids) {
		db.addField(SUBSET_IDS, ids);
	}

	public void setDistributionsIds(ArrayList<Integer> ids) {
		db.addField(DISTRIBUTIONS_IDS, ids);
	}

	public void addSubsetID(int id) {
		ArrayList<Integer> ids = (ArrayList<Integer>) db.getField(SUBSET_IDS);
		if (ids != null) {
			if (!ids.contains(id)) {
				ids.add(id);
				db.addField(SUBSET_IDS, ids);
			}
		} else {
			ids = new ArrayList<Integer>();
			ids.add(id);
			db.addField(SUBSET_IDS, ids);
		}
	}

	public void addDistributionID(int id) {
		ArrayList<Integer> ids = (ArrayList<Integer>) db.getField(DISTRIBUTIONS_IDS);
		if (ids != null) {
			if (!ids.contains(id)) {
				ids.add(id);
				db.addField(DISTRIBUTIONS_IDS, ids);
			}
		} else {
			ids = new ArrayList<Integer>();
			ids.add(id);
			db.addField(DISTRIBUTIONS_IDS, ids);
		}
	}

	public ArrayList<Integer> getDistributionsIDs() {
		return (ArrayList<Integer>) db.getField(DISTRIBUTIONS_IDS);
	}

	@JsonIgnore
	public ArrayList<DistributionDB> getDistributionsAsMongoDBObjects() {
		return new DatasetQueries().getDistributions(this);
	}

	public ArrayList<Integer> getSubsetsIDs() {
		return (ArrayList<Integer>) db.getField(SUBSET_IDS);
	}

	@JsonIgnore
	public ArrayList<DatasetDB> getSubsetsAsMongoDBObject() {
		return new DatasetQueries().getSubsetsAsMongoDBObject(this);
	}

	public void setDescriptionFileURL(String descriptionFileURL){
		db.addField(DESCRIPTION_FILE_URL, descriptionFileURL);
	}

	public String getDescriptionFileURL(){
		return db.getField(DESCRIPTION_FILE_URL).toString();
	}
	
	public ArrayList<Integer> getParentDatasetID() {
		try{
		ArrayList<Integer> parentDatasetsIDs = (ArrayList<Integer>) db.getField(PARENT_DATASETS);
		if (parentDatasetsIDs.get(0) != 0 || parentDatasetsIDs.size() >= 1)
			return parentDatasetsIDs;
		else
			return new ArrayList<Integer>();
		}
		catch(NullPointerException e){
			return new ArrayList<Integer>();
		}
	}

	public void addParentDatasetID(int id) {
		ArrayList<Integer> ids = (ArrayList<Integer>) db.getField(PARENT_DATASETS);
		if (ids != null) {
			if (!ids.contains(id)) {
				ids.add(id);
				db.addField(PARENT_DATASETS, ids);
			}
		} else {
			ids = new ArrayList<Integer>();
			ids.add(id);
			db.addField(PARENT_DATASETS, ids);
		}
	}

	public int getDatasetTriples() {
		BasicDBObject query = new BasicDBObject(DistributionDB.TOP_DATASET, getLODVaderID());
		DBCursor list = db.getCollection(DistributionDB.COLLECTION_NAME).find(query);
		int triples = 0;
		for (DBObject d : list) {
			if (d.get(DistributionDB.TRIPLES) != null)
				triples = triples + ((Number) d.get(DistributionDB.TRIPLES)).intValue();
		}
		return triples;
	}

}
