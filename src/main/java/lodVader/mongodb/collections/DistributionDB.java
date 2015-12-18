package lodVader.mongodb.collections;

import java.net.MalformedURLException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import com.mongodb.DBObject;

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
		find(true);
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

	// Distributions possible status
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

	public static final String TOP_DATASET = "topDataset";

	public static final String TOP_DATASET_TITLE = "topDatasetTitle";

	public static final String NUMBER_OF_SUBJECT_TRIPLES = "numberOfSubjectTriples";

	public static final String NUMBER_OF_OBJECTS_TRIPLES = "numberOfObjectTriples";

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

	public int getTopDatasetID() {
		return ((Number) getField(TOP_DATASET)).intValue();
	}

	public void setTopDataset(int topDataset) {
		addField(TOP_DATASET, topDataset);
	}

	public int getNumberOfSubjectTriples() {
		return ((Number) getField(NUMBER_OF_SUBJECT_TRIPLES)).intValue();
	}

	public void setNumberOfSubjectTriples(int numberOfSubjectTriples) {
		addField(NUMBER_OF_SUBJECT_TRIPLES, numberOfSubjectTriples);
	}

	public int getNumberOfObjectTriples() {
		return ((Number) getField(NUMBER_OF_OBJECTS_TRIPLES)).intValue();
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
		return ((Number) getField(TRIPLES)).intValue();
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

	public void setFormat(String format) {
		addField(FORMAT, format);
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

	public String getStatus() {
		try {
			getField(STATUS).toString();
		} catch (NullPointerException e) {
			return null;
		}
		return getField(STATUS).toString();
	}

	public void setStatus(String status) {
		addField(STATUS, status);
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
