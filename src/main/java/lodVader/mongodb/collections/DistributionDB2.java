package lodVader.mongodb.collections;

public class DistributionDB2 extends ResourceDB {

	// Collection name
	public static final String COLLECTION_NAME = "Distribution";

	public DistributionDB2() {
		super(COLLECTION_NAME);
		addPK(DOWNLOAD_URL);
		
		addMandatoryField(DOWNLOAD_URL);
		addMandatoryField(TOP_DATASET);		
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
	private static final String DOWNLOAD_URL = "downloadUrl";

	private static final String TOP_DATASET = "topDataset";

	private static final String TOP_DATASET_TITLE = "topDatasetTitle";

	private static final String NUMBER_OF_SUBJECT_TRIPLES = "numberOfSubjectTriples";

	private static final String NUMBER_OF_OBJECTS_TRIPLES = "numberOfObjectTriples";

	private static final String STATUS = "status";

	private static final String SUCCESSFULLY_DOWNLOADED = "successfullyDownloaded";

	private static final String LAST_MSG = "lastMsg";

	private static final String HTTP_BYTE_SIZE = "httpByteSize";

	private static final String HTTP_FORMAT = "httpFormat";

	private static final String HTTP_LAST_MODIFIED = "httpLastModified";

	private static final String TRIPLES = "triples";

	private static final String FORMAT = "format";

	private static final String RESOURCE_URI = "resourceUri";

	private static final String LAST_TIME_STREAMED = "lastTimeStreamed";

	public String getDownloadUrl() {
		return getField(DOWNLOAD_URL).toString();
	}

	public void setDownloadUrl(String downloadUrl) {
		addField(DOWNLOAD_URL, downloadUrl);
	}

	public String getHttpByteSize() {
		return getField(HTTP_BYTE_SIZE).toString();
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
		return getField(HTTP_FORMAT).toString();
	}

	public void setHttpFormat(String httpFormat) {
		addField(HTTP_FORMAT, httpFormat);
	}

	public String getHttpLastModified() {
		return getField(LAST_TIME_STREAMED).toString();
	}

	public void setHttpLastModified(String httpLastModified) {
		addField(HTTP_LAST_MODIFIED, httpLastModified);
	}

	public Integer getTriples() {
		return ((Number) getField(TRIPLES)).intValue();
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
		return getField(LAST_MSG).toString();
	}

	public void setLastMsg(String lastMsg) {
		addField(LAST_MSG, lastMsg);
	}

	public String getStatus() {
		return getField(STATUS).toString();
	}

	public void setStatus(String status) {
		addField(STATUS, status);
	}

	public boolean getIsVocabulary() {
		return Boolean.getBoolean(getField(IS_VOCABULARY).toString());
	}

	public String getResourceUri() {
		return getField(RESOURCE_URI).toString();
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

}
