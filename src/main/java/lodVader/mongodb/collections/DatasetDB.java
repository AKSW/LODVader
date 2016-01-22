package lodVader.mongodb.collections;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.mongodb.queries.DatasetQueries;

public class DatasetDB extends ResourceDB {

	// Collection name
	public static final String COLLECTION_NAME = "Dataset";

	public static final String PARENT_DATASETS = "parentDatasets";

	public static final String SUBSET_IDS = "subsetIDs";

	public static final String DISTRIBUTIONS_IDS = "distributionsIDs";

	public static final String OBJECT_FILENAME = "objectFileName";

	public static final String SUBJECT_FILTER_FILENAME = "subjectFileName";

	public static final String DESCRIPTION_FILE_URL = "descriptionFileURL";

	public DatasetDB(String uri) { 
		super(COLLECTION_NAME);
		setKeys();
		setUri(uri);
		find(true);
		if (getLODVaderID() == null)
			setLodVaderID(new LODVaderCounterDB().incrementAndGetID());
	}

	public DatasetDB(int id) {
		super(COLLECTION_NAME);
		setKeys();
		setLodVaderID(id);
		find(true);
		if (getLODVaderID() == null)
			setLodVaderID(new LODVaderCounterDB().incrementAndGetID());
	}

	public DatasetDB(DBObject object) {
		super(COLLECTION_NAME);
		setKeys();
		mongoDBObject = object;
	}

	public void setKeys() {
		addPK(URI);
		addPK(LOD_VADER_ID);
		addMandatoryField(URI);
		addMandatoryField(DESCRIPTION_FILE_URL);
//		addMandatoryField(SUBSET_IDS);
//		addMandatoryField(DISTRIBUTIONS_IDS);
	}

	public void setSubsetIds(ArrayList<Integer> ids) {
		addField(SUBSET_IDS, ids);
	}

	public void setDistributionsIds(ArrayList<Integer> ids) {
		addField(DISTRIBUTIONS_IDS, ids);
	}

	public void addSubsetID(int id) {
		ArrayList<Integer> ids = (ArrayList<Integer>) getField(SUBSET_IDS);
		if (ids != null) {
			if (!ids.contains(id)) {
				ids.add(id);
				addField(SUBSET_IDS, ids);
			}
		} else {
			ids = new ArrayList<Integer>();
			ids.add(id);
			addField(SUBSET_IDS, ids);
		}
	}

	public void addDistributionID(int id) {
		ArrayList<Integer> ids = (ArrayList<Integer>) getField(DISTRIBUTIONS_IDS);
		if (ids != null) {
			if (!ids.contains(id)) {
				ids.add(id);
				addField(DISTRIBUTIONS_IDS, ids);
			}
		} else {
			ids = new ArrayList<Integer>();
			ids.add(id);
			addField(DISTRIBUTIONS_IDS, ids);
		}
	}

	public ArrayList<Integer> getDistributionsIDs() {
		return (ArrayList<Integer>) getField(DISTRIBUTIONS_IDS);
	}

	@JsonIgnore
	public ArrayList<DistributionDB> getDistributionsAsMongoDBObjects() {
		return new DatasetQueries().getDistributions(this);
	}

	public ArrayList<Integer> getSubsetsIDs() {
		return (ArrayList<Integer>) getField(SUBSET_IDS);
	}

	@JsonIgnore
	public ArrayList<DatasetDB> getSubsetsAsMongoDBObject() {
		return new DatasetQueries().getSubsetsAsMongoDBObject(this);
	}

	public void setDescriptionFileURL(String descriptionFileURL){
		addField(DESCRIPTION_FILE_URL, descriptionFileURL);
	}

	public String getDescriptionFileURL(){
		return getField(DESCRIPTION_FILE_URL).toString();
	}
	
	public ArrayList<Integer> getParentDatasetID() {
		try{
		ArrayList<Integer> parentDatasetsIDs = (ArrayList<Integer>) getField(PARENT_DATASETS);
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
		ArrayList<Integer> ids = (ArrayList<Integer>) getField(PARENT_DATASETS);
		if (ids != null) {
			if (!ids.contains(id)) {
				ids.add(id);
				addField(PARENT_DATASETS, ids);
			}
		} else {
			ids = new ArrayList<Integer>();
			ids.add(id);
			addField(PARENT_DATASETS, ids);
		}
	}

	public int getDatasetTriples() {
		BasicDBObject query = new BasicDBObject(DistributionDB.TOP_DATASET, getLODVaderID());
		DBCursor list = getCollection(DistributionDB.COLLECTION_NAME).find(query);
		int triples = 0;
		for (DBObject d : list) {
			if (d.get(DistributionDB.TRIPLES) != null)
				triples = triples + ((Number) d.get(DistributionDB.TRIPLES)).intValue();
		}
		return triples;
	}

}
