package lodVader.mongodb.collections;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.queries.DatasetQueries;

public class DatasetDB extends ResourceDB {
	 
	// Collection name
	public static final String COLLECTION_NAME = "Dataset";
	
	public static final String PARENT_DATASETS = "parentDatasets";

	public static final String SUBSET_IDS = "subsetIDs";

	public static final String DISTRIBUTIONS_IDS = "distributionsIDs";

	public static final String DESCRIPTION_FILENAME = "descriptionFileName";
	
	public static final String OBJECT_FILENAME = "objectFileName";
	
	public static final String SUBJECT_FILTER_FILENAME = "subjectFileName";
	
	public static final String ACCESS_URL = "accessUrl";
	
	public static final String LOD_VADER_ID = "lodVaderID";
	
	

	// class properties

	private String descriptionFileName;

	private String access_url;

	private int lodVaderID = 0;
	

	public ArrayList<Integer> subsetsIDs = new ArrayList<Integer> ();

	public ArrayList<Integer>  distributionsIDs = new ArrayList<Integer> ();
	
	private ArrayList<Integer> parentDatasetsIDs = new ArrayList<Integer>();

	public DatasetDB(String uri) {
		super(COLLECTION_NAME, uri);
		loadObject();
	}
	public DatasetDB(String uri, boolean isRegex) {
		super(COLLECTION_NAME, uri, isRegex);
		loadObject();
	}
	
	public DatasetDB(int id) {
		super(COLLECTION_NAME, id);
		loadObject(id);
	}
	
	public DatasetDB(DBObject object) {
		super(COLLECTION_NAME, object);
		load(object);
	}

	public boolean updateObject(boolean checkBeforeInsert) {

		// save object case it doens't exists
		try {
			// updating subsets on mongodb
			mongoDBObject.put(SUBSET_IDS, subsetsIDs);
			
			if(lodVaderID == 0)
				lodVaderID = new LODVaderCounterDB().incrementAndGetID();
			mongoDBObject.put(LOD_VADER_ID, lodVaderID);
			
			// updating distributions on mongodb
			mongoDBObject.put(DISTRIBUTIONS_IDS, distributionsIDs);
	
			mongoDBObject.put(TITLE, title);

			mongoDBObject.put(DESCRIPTION_FILENAME, descriptionFileName);

			mongoDBObject.put(LABEL, label);
			
			mongoDBObject.put(IS_VOCABULARY, isVocabulary);
			
			mongoDBObject.put(PARENT_DATASETS, parentDatasetsIDs);
			
			mongoDBObject.put(ACCESS_URL, access_url);

			// adding timestamp value
			mongoDBObject.put(MODIFIED_TIMESTAMP, new Date());
			
			
			insert(checkBeforeInsert);
			return true;
		} catch (Exception e2) {
//			e2.printStackTrace();

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
	
	protected void load(DBObject obj){
		if (obj != null) {
			// mongoDBObject = (BasicDBObject) obj;

			label = (String) obj.get(LABEL);
			uri = (String) obj.get(URI);
			title = (String) obj.get(TITLE);
			descriptionFileName = (String) obj.get(DESCRIPTION_FILENAME);
			isVocabulary = (Boolean) obj.get(IS_VOCABULARY);
			access_url = (String) obj.get(ACCESS_URL);
			lodVaderID = (Integer) obj.get(LOD_VADER_ID);
			if(lodVaderID == 0)
				lodVaderID = new LODVaderCounterDB().incrementAndGetID();

			// loading subsets to object
			BasicDBList subsetList = (BasicDBList) obj.get(SUBSET_IDS);
			if(subsetsIDs == null)
				subsetsIDs = new ArrayList<Integer>();
			for (Object sd : subsetList) {
				subsetsIDs.add(((Number)sd).intValue());
			}

			// loading distributions to object
			BasicDBList distributionList = (BasicDBList) obj
					.get(DISTRIBUTIONS_IDS);
			if(distributionsIDs == null)
				distributionsIDs = new ArrayList<Integer>();
			for (Object sd : distributionList) {
				distributionsIDs.add(((Number) sd).intValue());
			}
			
			// loading parent datasets to object
			BasicDBList parentDatasetsList = (BasicDBList) obj
					.get(PARENT_DATASETS);
			if(parentDatasetsIDs == null)
				parentDatasetsIDs = new ArrayList<Integer>();
			for (Object sd : parentDatasetsList) {
				parentDatasetsIDs.add(((Number)(sd)).intValue());
			}
		}
	}

	protected boolean loadObject() {
		DBObject obj = search();
		load(obj); 
		return true;
	}

	protected boolean loadObject(int id) {
		DBObject obj = search(id);
		load(obj); 
		return true;
	}

	public void addSubsetID(int subsetID) {
		if (!subsetsIDs.contains(subsetID) && subsetID!=0)
			subsetsIDs.add(subsetID);
	}

	public void removeSubsetURI(String subsetURI) {
		if (subsetsIDs.contains(subsetURI) && subsetURI!=null)
			subsetsIDs.remove(subsetURI);
	}
	
	public void addDistributionID(int distributionURI) {
		if (!distributionsIDs.contains(distributionURI) && distributionURI!=0)
			distributionsIDs.add(distributionURI);
	}

	public void setLabel(String label) {
		this.label = label;
		mongoDBObject.put(LABEL, label);
	}

	public List<Integer> getDistributionsIDs() {
		return distributionsIDs;
	}
	
	public List<DistributionDB> getDistributionsAsMongoDBObjects() {
		return new DatasetQueries().getDistributionsAsMongoDBObject(this);
	}

	public List<Integer> getSubsetsIDs() {
		return subsetsIDs;
	}
	
	public ArrayList<DatasetDB> getSubsetsAsMongoDBObject(){
		
		return new DatasetQueries().getSubsetsAsMongoDBObject(this);
		
	}

	public String getLabel() {
		return label;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDescriptionFileName() {
		return descriptionFileName;
	}

	public void setDescriptionFileName(String descriptionFileName) {
		this.descriptionFileName = descriptionFileName;
	}
	
	public Boolean getIsVocabulary() {
		return isVocabulary;
	}
	
	public void setIsVocabulary(Boolean isVocabulary) {
		this.isVocabulary = isVocabulary;
	}
	
	public ArrayList<Integer> getParentDatasetID() {
		if(parentDatasetsIDs.get(0) != 0 || parentDatasetsIDs.size()>=1)
		return parentDatasetsIDs;
		else return new  ArrayList<Integer>();
	}
	public void addParentDatasetID(int parentDatasetURI) {
		if (!parentDatasetsIDs.contains(parentDatasetURI))
			parentDatasetsIDs.add(parentDatasetURI);
	}

	public void removeParentDatasetURI(String parentDatasetURI) {
		if (parentDatasetsIDs.contains(parentDatasetURI) && parentDatasetURI!=null)
			parentDatasetsIDs.remove(parentDatasetURI);
	}

	public String getAccess_url() {
		return access_url;
	}

	public void setAccess_url(String access_url) {
		this.access_url = access_url;
	}
	
	public int getLODVaderID() {
		if(lodVaderID == 0)
			lodVaderID = new LODVaderCounterDB().incrementAndGetID();
		return lodVaderID;
	}

}
