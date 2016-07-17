package lodVader.mongodb.collections;

import java.net.MalformedURLException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import com.mongodb.DBObject;

import lodVader.enumerators.DistributionStatus;
import lodVader.mongodb.queries.DatasetQueries;
import lodVader.utils.URLUtils;

public class DistributionDB extends ResourceDB {

	// Collection name
	public static final String COLLECTION_NAME = "Distribution";

	public DistributionDB() {
		super(COLLECTION_NAME);
		setKeys();
		setLodVaderID(new LODVaderCounterDB().incrementAndGetID());
	}

	public DistributionDB(DBObject object) {
		super(COLLECTION_NAME);
		mongoDBObject = object;
		setKeys();
	}

	public DistributionDB(int id) {
		super(COLLECTION_NAME);
		setKeys();
		setLodVaderID(id);
		find(true, LOD_VADER_ID, id);
		if (getLODVaderID() == null)
			setLodVaderID(new LODVaderCounterDB().incrementAndGetID());
	}

	public DistributionDB(String uri) throws MalformedURLException {
		super(COLLECTION_NAME);
		setKeys();
		setUri(uri);
		setDownloadUrl(uri);
		find(true);
		if (getLODVaderID() == null)
			setLodVaderID(new LODVaderCounterDB().incrementAndGetID());
	}

	private void setKeys() {
		addPK(URI);
		addPK(DOWNLOAD_URL);
		addPK(LOD_VADER_ID);
		addMandatoryField(DOWNLOAD_URL);
		addMandatoryField(URI);
		addMandatoryField(LOD_VADER_ID);
		addMandatoryField(STATUS);
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
		return getField(DOWNLOAD_URL).toString();
	}

	public void setDownloadUrl(String downloadUrl) throws MalformedURLException {
		URLUtils utils = new URLUtils();
		utils.validateURL(downloadUrl);
		addField(DOWNLOAD_URL, downloadUrl);
	}

	public String getHttpByteSize() {
		try {
			return getField(HTTP_BYTE_SIZE).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setHttpByteSize(String httpByteSize) {
		addField(HTTP_BYTE_SIZE, httpByteSize);
	}

	public void setObjectCohesion(int objectCohesion) {
		addField(OBJECTS_COHESION, objectCohesion);
	}

	public int getObjectCohesion() {
		try {
			return ((Number) getField(OBJECTS_COHESION)).intValue();
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public Integer getTopDatasetID() {
		return ((Number) getField(TOP_DATASET)).intValue();
	}

	public void setTopDataset(int topDataset) {
		addField(TOP_DATASET, topDataset);
	}

	public int getNumberOfSubjectTriples() {
		try {
			return ((Number) getField(NUMBER_OF_SUBJECT_TRIPLES)).intValue();
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public void setNumberOfSubjectTriples(int numberOfSubjectTriples) {
		addField(NUMBER_OF_SUBJECT_TRIPLES, numberOfSubjectTriples);
	}

	public int getNumberOfObjectTriples() {
		if (getField(NUMBER_OF_OBJECTS_TRIPLES) != null)
			return ((Number) getField(NUMBER_OF_OBJECTS_TRIPLES)).intValue();
		else
			return 0;
	}

	public void setNumberOfObjectTriples(int numberOfObjectTriples) {
		addField(NUMBER_OF_OBJECTS_TRIPLES, numberOfObjectTriples);
	}

	public String getHttpFormat() {
		try {
			return getField(HTTP_FORMAT).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setHttpFormat(String httpFormat) {
		addField(HTTP_FORMAT, httpFormat);
	}

	public String getHttpLastModified() {
		try {
			return getField(LAST_TIME_STREAMED).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setHttpLastModified(String httpLastModified) {
		addField(HTTP_LAST_MODIFIED, httpLastModified);
	}

	public Integer getTriples() {
		if (getField(TRIPLES) != null)
			return ((Number) getField(TRIPLES)).intValue();
		else
			return 0;
	}

	public String getTriplesStringFormat() {
		DecimalFormat formatter = new DecimalFormat("#,###,###,###,###");
		return formatter.format(((Number) getField(TRIPLES)).intValue());
	}

	public void setTriples(Integer triples) {
		addField(TRIPLES, triples);
	}

	public String getFormat() {
		return getField(FORMAT).toString();
	}

	public String getUndefinedLinks() {
		try {
			return getField(UNDEFINED_LINKS).toString();
		} catch (NullPointerException e) {
			return "0";
		}
	}

	public void setFormat(String format) {
		addField(FORMAT, format);
	}

	public void setUndefinedLinks(Double undefinedLinks) {
		addField(UNDEFINED_LINKS, undefinedLinks);
	}

	public boolean getSuccessfullyDownloaded() {
		return Boolean.getBoolean(getField(SUCCESSFULLY_DOWNLOADED).toString());
	}

	public void setSuccessfullyDownloaded(boolean successfullyDownloaded) {
		addField(SUCCESSFULLY_DOWNLOADED, successfullyDownloaded);
	}

	public String getLastMsg() {

		if (getField(LAST_MSG) == null)
			return "";
		return getField(LAST_MSG).toString();
	}

	public void setLastMsg(String lastMsg) {
		addField(LAST_MSG, lastMsg);
	}

	public DistributionStatus getStatus() {
		try {
			getField(STATUS).toString();
		} catch (NullPointerException e) {
			return null;
		}
		return DistributionStatus.valueOf(getField(STATUS).toString());
	}

	public void setStatus(DistributionStatus status) {
		addField(STATUS, status.name());
	}

	public boolean getIsVocabulary() {
		return Boolean.parseBoolean(getField(IS_VOCABULARY).toString());
	}

	public String getResourceUri() {
		try {
			return getField(RESOURCE_URI).toString();
		} catch (NullPointerException e) {
			return "";
		}
	}

	public void setResourceUri(String resourceUri) {
		addField(RESOURCE_URI, resourceUri);
	}

	public String getLastTimeStreamed() {
		return getField(LAST_TIME_STREAMED).toString();
	}

	public void setLastTimeStreamed(String lastTimeStreamed) {
		addField(LAST_TIME_STREAMED, lastTimeStreamed);
	}

	public String getTopDatasetTitle() {
		return getField(TOP_DATASET_TITLE).toString();
	}

	public void setTopDatasetTitle(String topDatasetTitle) {
		addField(TOP_DATASET_TITLE, topDatasetTitle);
	}

	public void setDefaultDatasets(ArrayList<Integer> defaultDatasets) {
		addField(DEFAULT_DATASETS, defaultDatasets);
	}

	public void setSubjectFile(String subjectFile) {
		addField(SUBJECT_FILE, subjectFile);
	}

	public void setObjectFile(String objectFile) {
		addField(OBJECT_FILE, objectFile);
	}

	public String getSubjectFile() {
		if (getField(SUBJECT_FILE) != null)
			return getField(SUBJECT_FILE).toString();
		else
			return "";
	}

	public String getObjectFile() {
		if (getField(OBJECT_FILE) != null)
			return getField(OBJECT_FILE).toString();
		else
			return "";
	}

	public void addDefaultDatasets(int datasetID) {
		ArrayList<Integer> ids = (ArrayList<Integer>) getField(DEFAULT_DATASETS);
		if (ids != null) {
			if (!ids.contains(datasetID)) {
				ids.add(datasetID);
				addField(DEFAULT_DATASETS, ids);
			}
		} else {
			ids = new ArrayList<Integer>();
			ids.add(datasetID);
			addField(DEFAULT_DATASETS, ids);
		}
	}

	public ArrayList<Integer> getDefaultDatasets() {
		return (ArrayList<Integer>) getField(DEFAULT_DATASETS);
	}

	public ArrayList<DatasetDB> getDefaultDatasetsAsResources() {
		return new DatasetQueries().getDatasets((ArrayList<Integer>) getField(DEFAULT_DATASETS));
	}

}
