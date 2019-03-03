package lodVader.mongodb.collections;

import java.net.MalformedURLException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Autowired;

import com.mongodb.DBObject;

import lodVader.enumerators.DistributionStatus;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.queries.DatasetQueries;
import lodVader.utils.URLUtils;

public class DistributionDB extends ResourceDB {
	
	@Autowired
	LODVaderCounterDB lovVaderCounterDB;

	// Collection name
	public static final String COLLECTION_NAME = "Distribution";

	public DistributionDB(DBSuperClass2 db) {
		super(db, COLLECTION_NAME);
		super.db=db;
	}
	
	public void init() {
		setKeys();
		setLodVaderID(lovVaderCounterDB.incrementAndGetID());
	}

	public void init(DBObject object) {
		db.mongoDBObject = object;
		setKeys();
	}

	public void init(int id) {
		setKeys();
		setLodVaderID(id);
		db.find(true, LOD_VADER_ID, id);
		if (getLODVaderID() == null)
			setLodVaderID(lovVaderCounterDB.incrementAndGetID());
	}

	public void init(String uri) throws MalformedURLException {
		setKeys();
		setUri(uri);
		setDownloadUrl(uri);
		db.find(true);
		if (getLODVaderID() == null)
			setLodVaderID(lovVaderCounterDB.incrementAndGetID());
	}

	private void setKeys() {
		db.addPK(URI);
		db.addPK(DOWNLOAD_URL);
		db.addPK(LOD_VADER_ID);
		db.addMandatoryField(DOWNLOAD_URL);
		db.addMandatoryField(URI);
		db.addMandatoryField(LOD_VADER_ID);
		db.addMandatoryField(STATUS);
	}

	// collection properties
	public static final String DOWNLOAD_URL = "downloadUrl";

	public static final String TOP_DATASET = "topDataset";

	public static final String TOP_DATASET_TITLE = "topDatasetTitle";

	public static final String NUMBER_OF_SUBJECT_TRIPLES = "numberOfSubjectTriples";

	public static final String NUMBER_OF_OBJECTS_TRIPLES = "numberOfObjectTriples";

	public static final String OBJECTS_COHESION = "objectCohesion";

	public static final String STATUS = "status";

	public static final String SUCCESSFULLY_DOWNLOADED = "successfullyDownloaded";

	public static final String LAST_MSG = "lastMsg";

	public static final String HTTP_BYTE_SIZE = "httpByteSize";

	public static final String HTTP_FORMAT = "httpFormat";

	public static final String HTTP_LAST_MODIFIED = "httpLastModified";

	public static final String TRIPLES = "triples";

	public static final String FORMAT = "format";

	public static final String RESOURCE_URI = "resourceUri";

	public static final String DEFAULT_DATASETS = "defaultDatasets";

	public static final String LAST_TIME_STREAMED = "lastTimeStreamed";

	public static final String UNDEFINED_LINKS = "undefinedLinks";

	public static final String OBJECT_FILE = "objectFile";

	public static final String SUBJECT_FILE = "subjectFile";

	public String getDownloadUrl() {
		return db.getField(DOWNLOAD_URL).toString();
	}

	public void setDownloadUrl(String downloadUrl) throws MalformedURLException {
		URLUtils utils = new URLUtils();
		utils.validateURL(downloadUrl);
		db.addField(DOWNLOAD_URL, downloadUrl);
	}

	public String getHttpByteSize() {
		try {
			return db.getField(HTTP_BYTE_SIZE).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setHttpByteSize(String httpByteSize) {
		db.addField(HTTP_BYTE_SIZE, httpByteSize);
	}

	public void setObjectCohesion(int objectCohesion) {
		db.addField(OBJECTS_COHESION, objectCohesion);
	}

	public int getObjectCohesion() {
		try {
			return ((Number) db.getField(OBJECTS_COHESION)).intValue();
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public Integer getTopDatasetID() {
		return ((Number) db.getField(TOP_DATASET)).intValue();
	}

	public void setTopDataset(int topDataset) {
		db.addField(TOP_DATASET, topDataset);
	}

	public int getNumberOfSubjectTriples() {
		try {
			return ((Number) db.getField(NUMBER_OF_SUBJECT_TRIPLES)).intValue();
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public void setNumberOfSubjectTriples(int numberOfSubjectTriples) {
		db.addField(NUMBER_OF_SUBJECT_TRIPLES, numberOfSubjectTriples);
	}

	public int getNumberOfObjectTriples() {
		if (db.getField(NUMBER_OF_OBJECTS_TRIPLES) != null)
			return ((Number) db.getField(NUMBER_OF_OBJECTS_TRIPLES)).intValue();
		else
			return 0;
	}

	public void setNumberOfObjectTriples(int numberOfObjectTriples) {
		db.addField(NUMBER_OF_OBJECTS_TRIPLES, numberOfObjectTriples);
	}

	public String getHttpFormat() {
		try {
			return db.getField(HTTP_FORMAT).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setHttpFormat(String httpFormat) {
		db.addField(HTTP_FORMAT, httpFormat);
	}

	public String getHttpLastModified() {
		try {
			return db.getField(LAST_TIME_STREAMED).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setHttpLastModified(String httpLastModified) {
		db.addField(HTTP_LAST_MODIFIED, httpLastModified);
	}

	public Integer getTriples() {
		if (db.getField(TRIPLES) != null)
			return ((Number) db.getField(TRIPLES)).intValue();
		else
			return 0;
	}

	public String getTriplesStringFormat() {
		DecimalFormat formatter = new DecimalFormat("#,###,###,###,###");
		return formatter.format(((Number) db.getField(TRIPLES)).intValue());
	}

	public void setTriples(Integer triples) {
		db.addField(TRIPLES, triples);
	}

	public String getFormat() {
		return db.getField(FORMAT).toString();
	}

	public String getUndefinedLinks() {
		try {
			return db.getField(UNDEFINED_LINKS).toString();
		} catch (NullPointerException e) {
			return "0";
		}
	}

	public void setFormat(String format) {
		db.addField(FORMAT, format);
	}

	public void setUndefinedLinks(Double undefinedLinks) {
		db.addField(UNDEFINED_LINKS, undefinedLinks);
	}

	public boolean getSuccessfullyDownloaded() {
		return Boolean.getBoolean(db.getField(SUCCESSFULLY_DOWNLOADED).toString());
	}

	public void setSuccessfullyDownloaded(boolean successfullyDownloaded) {
		db.addField(SUCCESSFULLY_DOWNLOADED, successfullyDownloaded);
	}

	public String getLastMsg() {

		if (db.getField(LAST_MSG) == null)
			return "";
		return db.getField(LAST_MSG).toString();
	}

	public void setLastMsg(String lastMsg) {
		db.addField(LAST_MSG, lastMsg);
	}

	public DistributionStatus getStatus() {
		try {
			db.getField(STATUS).toString();
		} catch (NullPointerException e) {
			return null;
		}
		return DistributionStatus.valueOf(db.getField(STATUS).toString());
	}

	public void setStatus(DistributionStatus status) {
		db.addField(STATUS, status.name());
	}

	public boolean getIsVocabulary() {
		return Boolean.parseBoolean(db.getField(IS_VOCABULARY).toString());
	}

	public String getResourceUri() {
		try {
			return db.getField(RESOURCE_URI).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setResourceUri(String resourceUri) {
		db.addField(RESOURCE_URI, resourceUri);
	}

	public String getLastTimeStreamed() {
		return db.getField(LAST_TIME_STREAMED).toString();
	}

	public void setLastTimeStreamed(String lastTimeStreamed) {
		db.addField(LAST_TIME_STREAMED, lastTimeStreamed);
	}

	public String getTopDatasetTitle() {
		return db.getField(TOP_DATASET_TITLE).toString();
	}

	public void setTopDatasetTitle(String topDatasetTitle) {
		db.addField(TOP_DATASET_TITLE, topDatasetTitle);
	}

	public void setDefaultDatasets(ArrayList<Integer> defaultDatasets) {
		db.addField(DEFAULT_DATASETS, defaultDatasets);
	}

	public void setSubjectFile(String subjectFile) {
		db.addField(SUBJECT_FILE, subjectFile);
	}

	public void setObjectFile(String objectFile) {
		db.addField(OBJECT_FILE, objectFile);
	}

	public String getSubjectFile() {
		if (db.getField(SUBJECT_FILE) != null)
			return db.getField(SUBJECT_FILE).toString();
		else
			return "";
	}

	public String getObjectFile() {
		if (db.getField(OBJECT_FILE) != null)
			return db.getField(OBJECT_FILE).toString();
		else
			return "";
	}

	public void addDefaultDatasets(int datasetID) {
		ArrayList<Integer> ids = (ArrayList<Integer>) db.getField(DEFAULT_DATASETS);
		if (ids != null) {
			if (!ids.contains(datasetID)) {
				ids.add(datasetID);
				db.addField(DEFAULT_DATASETS, ids);
			}
		} else {
			ids = new ArrayList<Integer>();
			ids.add(datasetID);
			db.addField(DEFAULT_DATASETS, ids);
		}
	}

	public ArrayList<Integer> getDefaultDatasets() {
		return (ArrayList<Integer>) db.getField(DEFAULT_DATASETS);
	}

	public ArrayList<DatasetDB> getDefaultDatasetsAsResources() {
		return new DatasetQueries().getDatasets((ArrayList<Integer>) db.getField(DEFAULT_DATASETS));
	}

}
