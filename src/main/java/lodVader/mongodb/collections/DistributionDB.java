package lodVader.mongodb.collections;

import java.util.ArrayList;
import java.util.Date;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.queries.DatasetQueries;

public class DistributionDB extends ResourceDB {

	// Collection name
	public static final String COLLECTION_NAME = "Distribution";

	
	// Distributions status on the system

	public static final String STATUS_STREAMING = "STREAMING";

	public static final String STATUS_STREAMED = "STREAMED";

	public static final String STATUS_SEPARATING_SUBJECTS_AND_OBJECTS = "SEPARATING_SUBJECTS_AND_OBJECTS";

	public static final String STATUS_WAITING_TO_STREAM = "WAITING_TO_STREAM";
	
	public static final String STATUS_CREATING_BLOOM_FILTER = "CREATING_BLOOM_FILTER";
	
	public static final String STATUS_CREATING_LINKSETS = "CREATING_LINKSETS";
	
	public static final String STATUS_ERROR = "ERROR";
	
	public static final String STATUS_DONE = "DONE";
	
	public static final String STATUS_CREATING_JACCARD_SIMILARITY = "CREATING_JACCARD_SIMILARITY";
	
	public static final String STATUS_UPDATING_LINK_STRENGTH = "UPDATING_LINK_STRENGTH";
	
	
	// collection properties
	public static final String DOWNLOAD_URL = "downloadUrl";

	public static final String DEFAULT_DATASETS = "defaultDatasets";

	public static final String TOP_DATASET = "topDataset";

	public static final String TOP_DATASET_TITLE = "topDatasetTitle";

	public static final String SUBJECT_FILTER_PATH = "subjectFilterPath";

	public static final String OBJECT_FILTER_PATH = "objectFilterPath";

	public static final String OBJECT_PATH = "objectPath";

	public static final String NUMBER_OF_SUBJECT_TRIPLES = "numberOfSubjectTriples";

	public static final String NUMBER_OF_OBJECTS_TRIPLES = "numberOfObjectTriples";

	public static final String TIME_TO_CREATE_SUBJECT_FILTER = "timeToCreateSubjectFilter";
	
	public static final String TIME_TO_CREATE_OBJECT_FILTER = "timeToCreateObjectFilter";
	
	public static final String STATUS = "status";
	
	public static final String SUCCESSFULLY_DOWNLOADED = "successfullyDownloaded";
	
	public static final String LAST_MSG = "lastMsg";


	public static final String HTTP_BYTE_SIZE = "httpByteSize";

	public static final String HTTP_FORMAT = "httpFormat";

	public static final String HTTP_LAST_MODIFIED = "httpLastModified";

	public static final String TRIPLES = "triples";
	
	public static final String FORMAT = "format";
	
	public static final String RESOURCE_URI = "resourceUri";
	
	public static final String LAST_TIME_STREAMED = "lastTimeStreamed";
	
	

	
	private ArrayList<Integer> defaultDatasets = new ArrayList<Integer>();

	private String downloadUrl;

	private int topDataset;

	private String subjectFilterPath;

	private String topDatasetTitle;

	private String objectFilterPath;

	private String objectPath;

	private String numberOfSubjectTriples;

	private String numberOfObjectTriples;

	private String timeToCreateObjectFilter;
	
	private String timeToCreateSubjectFilter;

	private String httpByteSize;

	private String httpFormat;

	private String httpLastModified;

	private Integer triples = 0;

	private String format;
	
	private boolean successfullyDownloaded;
	
	private String lastMsg = "";
//	private String lastErrorMsg = "";
	
	private String status;
		
	private String resourceUri;
	
	private String lastTimeStreamed;
	
	

	public DistributionDB(String uri) {
		super(COLLECTION_NAME, uri);
		loadObject();
	}
	
	public DistributionDB(int id) {
		super(COLLECTION_NAME, id);
		loadObject(id);
	}
	
	public DistributionDB(DBObject object) {
		super(COLLECTION_NAME, object);
		load(object);
	}


	public void addDefaultDataset(int defaultDataset) {
		if (!defaultDatasets.contains(defaultDataset))
			this.defaultDatasets.add(defaultDataset);
	}
	
	public void removeDefaultDataset(String defaultDataset) {
		if (defaultDatasets.contains(defaultDataset))
			this.defaultDatasets.remove(defaultDataset);
	}

	public boolean updateObject(boolean checkBeforeInsert) {
		// save object case it doens't exists
		try {
			mongoDBObject.put(DOWNLOAD_URL, downloadUrl);
			mongoDBObject.put(DEFAULT_DATASETS, defaultDatasets);
			mongoDBObject.put(HTTP_BYTE_SIZE, httpByteSize);
			mongoDBObject.put(HTTP_FORMAT, httpFormat);
			mongoDBObject.put(HTTP_LAST_MODIFIED, httpLastModified);
			mongoDBObject.put(TRIPLES, triples);
			mongoDBObject.put(TOP_DATASET, topDataset);
			mongoDBObject.put(TOP_DATASET_TITLE, topDatasetTitle);
			mongoDBObject.put(LAST_TIME_STREAMED, lastTimeStreamed);
			mongoDBObject.put(SUBJECT_FILTER_PATH, subjectFilterPath);
			mongoDBObject.put(OBJECT_FILTER_PATH, objectFilterPath);
			mongoDBObject.put(OBJECT_PATH, objectPath);
			mongoDBObject.put(NUMBER_OF_SUBJECT_TRIPLES,
					numberOfSubjectTriples);
			mongoDBObject.put(NUMBER_OF_OBJECTS_TRIPLES, numberOfObjectTriples);
			mongoDBObject.put(TIME_TO_CREATE_OBJECT_FILTER, timeToCreateObjectFilter);
			mongoDBObject.put(TIME_TO_CREATE_SUBJECT_FILTER, timeToCreateSubjectFilter);
			mongoDBObject.put(TITLE, title);
			mongoDBObject.put(FORMAT, format);	
			mongoDBObject.put(STATUS, status);	
			mongoDBObject.put(RESOURCE_URI, resourceUri);
			mongoDBObject.put(SUCCESSFULLY_DOWNLOADED, successfullyDownloaded);
			mongoDBObject.put(IS_VOCABULARY, isVocabulary);
			mongoDBObject.put(LAST_MSG, lastMsg);

			// adding timestamp value
			mongoDBObject.put(MODIFIED_TIMESTAMP, new Date());
			
			
			if(lodVaderID == 0)
				lodVaderID = new LODVaderCounterDB().incrementAndGetID();
			mongoDBObject.put(LOD_VADER_ID, lodVaderID);

			insert(checkBeforeInsert);

		} catch (Exception e2) {
			// e2.printStackTrace();
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
		return false;
	}

	protected void loadObject(int id) {
		DBObject obj = search(id);
		load(obj);
	}
	
	@Override
	protected boolean loadObject() {
		DBObject obj = search();
		load(obj);
		return true;
		
	}
	
	protected void load(DBObject obj){

		if (obj != null) {
			downloadUrl = (String) obj.get(DOWNLOAD_URL);
			httpByteSize = (String) obj.get(HTTP_BYTE_SIZE);
			topDataset = ((Number) obj.get(TOP_DATASET)).intValue();
			subjectFilterPath = (String) obj.get(SUBJECT_FILTER_PATH);
			objectFilterPath = (String) obj.get(OBJECT_FILTER_PATH);
			objectPath = (String) obj.get(OBJECT_PATH);
			title = (String) obj.get(TITLE);
			topDatasetTitle = (String) obj.get(TOP_DATASET_TITLE);
			httpFormat = (String) obj.get(HTTP_FORMAT);
			httpLastModified = (String) obj.get(HTTP_LAST_MODIFIED);
			format = (String) obj.get(FORMAT);
			lastTimeStreamed = (String) obj.get(LAST_TIME_STREAMED);
			status = (String) obj.get(STATUS);
			timeToCreateObjectFilter = (String) obj.get(TIME_TO_CREATE_OBJECT_FILTER);
			timeToCreateSubjectFilter = (String) obj.get(TIME_TO_CREATE_SUBJECT_FILTER);
//			((Number) mapObj.get("autostart")).intValue();
			triples = ((Number) obj.get(TRIPLES)).intValue() ;
			numberOfSubjectTriples = (String) obj
					.get(NUMBER_OF_SUBJECT_TRIPLES);
			numberOfObjectTriples = (String) obj.get(NUMBER_OF_OBJECTS_TRIPLES);
			resourceUri = (String) obj.get(RESOURCE_URI);
			successfullyDownloaded = (Boolean) obj.get(SUCCESSFULLY_DOWNLOADED);
			isVocabulary = (Boolean) obj.get(IS_VOCABULARY);
			lastMsg = (String) obj.get(LAST_MSG);
			
			lodVaderID = (Integer) obj.get(LOD_VADER_ID);
			if(lodVaderID == 0)
				lodVaderID = new LODVaderCounterDB().incrementAndGetID();
			
			// loading default datasets to object
			BasicDBList defaultDatasetList = (BasicDBList) obj
					.get(DEFAULT_DATASETS);
			if (defaultDatasetList != null)
				for (Object sd : defaultDatasetList) {
					defaultDatasets.add((((Number) sd)).intValue());
				}

		}
	}
	
	public String getDownloadUrl() {
		return downloadUrl;
	}

	public void setDownloadUrl(String downloadUrl) {
		this.downloadUrl = downloadUrl;
	}

	public String getHttpByteSize() {
		return httpByteSize;
	}

	public void setHttpByteSize(String httpByteSize) {
		this.httpByteSize = httpByteSize;
	}

	public int getTopDataset() {
		return topDataset;
	}

	public void setTopDataset(int topDataset) {
		this.topDataset = topDataset;
	}

	public String getSubjectFilterPath() {
		return subjectFilterPath;
	}

	public void setSubjectFilterPath(String subjectFilterPath) {
		this.subjectFilterPath = subjectFilterPath;
	}

	public String getObjectPath() {
		return objectPath;
	}

	public void setObjectPath(String objectPath) {
		this.objectPath = objectPath;
	}



	public String getNumberOfSubjectTriples() {
		return numberOfSubjectTriples;
	}


	public void setNumberOfSubjectTriples(String numberOfSubjectTriples) {
		this.numberOfSubjectTriples = numberOfSubjectTriples;
	}


	public String getNumberOfObjectTriples() {
		return numberOfObjectTriples;
	}

	public void setNumberOfObjectTriples(String numberOfObjectTriples) {
		this.numberOfObjectTriples = numberOfObjectTriples;
	}

	public ArrayList<Integer> getDefaultDatasets() {
		return defaultDatasets;
	}

	
	public ArrayList<DatasetDB> getDefaultDatasetsAsResources() {
		return new DatasetQueries().getDatasets(defaultDatasets);
	}

	
	public void setDefaultDatasets(ArrayList<Integer> defaultDatasets) {
		this.defaultDatasets = defaultDatasets;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getHttpFormat() {
		return httpFormat;
	}

	public void setHttpFormat(String httpFormat) {
		this.httpFormat = httpFormat;
	}

	public String getHttpLastModified() {
		return httpLastModified;
	}

	public void setHttpLastModified(String httpLastModified) {
		this.httpLastModified = httpLastModified;
	}

	public Integer getTriples() {
		return triples;
	}

	public void setTriples(Integer triples) {
		this.triples = triples;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public boolean getSuccessfullyDownloaded() {
		return successfullyDownloaded;
	}

	public void setSuccessfullyDownloaded(boolean successfullyDownloaded) {
		this.successfullyDownloaded = successfullyDownloaded;
	}



	public String getLastMsg() {
		return lastMsg;
	}


	public void setLastMsg(String lastMsg) {
		this.lastMsg = lastMsg;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public boolean getIsVocabulary() {
		return isVocabulary;
	}

	public void setIsVocabulary(boolean isVocabulary) {
		this.isVocabulary = isVocabulary;
	}

	public String getResourceUri() {
		return resourceUri;
	}

	public void setResourceUri(String resourceUri) {
		this.resourceUri = resourceUri;
	}

	public String getObjectFilterPath() {
		return objectFilterPath;
	}

	public void setObjectFilterPath(String objectFilterPath) {
		this.objectFilterPath = objectFilterPath;
	}


	public String getTimeToCreateObjectFilter() {
		return timeToCreateObjectFilter;
	}


	public void setTimeToCreateObjectFilter(String timeToCreateObjectFilter) {
		this.timeToCreateObjectFilter = timeToCreateObjectFilter;
	}


	public String getTimeToCreateSubjectFilter() {
		return timeToCreateSubjectFilter;
	}


	public void setTimeToCreateSubjectFilter(String timeToCreateSubjectFilter) {
		this.timeToCreateSubjectFilter = timeToCreateSubjectFilter;
	}


	public String getLastTimeStreamed() {
		return lastTimeStreamed;
	}


	public void setLastTimeStreamed(String lastTimeStreamed) {
		this.lastTimeStreamed = lastTimeStreamed;
	}

	public int getLODVaderID() {
		return lodVaderID;
	}

	public void setLODVaderID(int lodVaderID) {
		this.lodVaderID = lodVaderID;
	}

	public String getTopDatasetTitle() {
		return topDatasetTitle;
	}

	public void setTopDatasetTitle(String topDatasetTitle) {
		this.topDatasetTitle = topDatasetTitle;
	}
	
	
	

}
