package lodVader.mongodb.collections;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass2;

public class DatasetLinksetDB extends DBSuperClass2 {

	public DatasetLinksetDB() {
		super(COLLECTION_NAME);
	}

	// Collection name
	public static final String COLLECTION_NAME = "DatasetLinkset";

	// class properties
	public static final String LINKSET_ID = "linksetId";

	public static final String DISTRIBUTION_TARGET = "distributionTarget";

	public static final String DISTRIBUTION_SOURCE = "distributionSource";

	public static final String DISTRIBUTION_TARGET_IS_VOCABULARY = "distributionTargetIsVocabulary";

	public static final String DISTRIBUTION_SOURCE_IS_VOCABULARY = "distributionSourceIsVocabulary";
	
	public static final String DATASET_TARGET = "datasetTarget";

	public static final String DATASET_SOURCE = "datasetSource";

	public static final String DEAD_LINKS = "deadLinks";
	
	public static final String LINKS = "links";
	
	
	
	public DatasetLinksetDB(DBObject object) {
		super(COLLECTION_NAME);
		initializeVariables();
		mongoDBObject = object;
	}

	public DatasetLinksetDB(String id) {
		super(COLLECTION_NAME);
		initializeVariables();
		setLinksetID(id);
		find(true);
	}

	private void initializeVariables() {
		addPK(LINKSET_ID);
		addMandatoryField(LINKSET_ID);
		addMandatoryField(DISTRIBUTION_SOURCE);
		addMandatoryField(DISTRIBUTION_TARGET);
		addMandatoryField(DATASET_SOURCE);
		addMandatoryField(DATASET_TARGET);
		addMandatoryField(LINKS);
		// addMandatoryField(LINK_STRENGHT);
		// addMandatoryField(INVALID_LINKS);
		// addMandatoryField(PREDICATE_SIMILARITY);
		// addMandatoryField(OWL_CLASS_SIMILARITY);
		// addMandatoryField(RDF_SUBCLASS_SIMILARITY);
		// addMandatoryField(RDF_TYPE_SIMILARITY);
	}

	public int getDistributionTarget() {
		return Integer.parseInt(getField(DISTRIBUTION_TARGET).toString());
	}

	public void setDistributionTarget(int distributionTarget) {
		addField(DISTRIBUTION_TARGET, distributionTarget);
	}

	public String getLinksetID() {
		return getField(LINKSET_ID).toString();
	}

	public void setLinksetID(String id) {
		addField(LINKSET_ID, id);
	}

	public int getDistributionSource() {
		return Integer.parseInt(getField(DISTRIBUTION_SOURCE).toString());
	}

	public void setDistributionSource(int distributionSource) {
		addField(DISTRIBUTION_SOURCE, distributionSource);
	}

	public int getDatasetTarget() {
		return Integer.parseInt(getField(DATASET_TARGET).toString());
	}

	public void setDatasetTarget(int datasetTarget) {
		addField(DATASET_TARGET, datasetTarget);
	}

	public int getDatasetSource() {
		return Integer.parseInt(getField(DATASET_SOURCE).toString());
	}

	public void setDatasetSource(int datasetSource) {
		addField(DATASET_SOURCE, datasetSource);
	}

	public int getLinks() {
		try {
			return Integer.parseInt(getField(LINKS).toString());
		} catch (NullPointerException e) {
			setLinks(0);
			return 0;
		}
	}

	public String getLinksAsString() {
		return String.valueOf(getField(LINKS));
	}

	public void setLinks(int links) {
		addField(LINKS, links);
	}
	
	public void setDistributionTargetIsVocabulary(Boolean isVocabulary) {
		addField(DISTRIBUTION_TARGET_IS_VOCABULARY, isVocabulary);
	}
	
	public void setDistributionSourceIsVocabulary(Boolean isVocabulary) {
		addField(DISTRIBUTION_SOURCE_IS_VOCABULARY, isVocabulary);
	}
	
	public Boolean getDistributionTargetIsVocabulary() {
		return (Boolean) getField(DISTRIBUTION_TARGET_IS_VOCABULARY);
	}
	
	public Boolean getDistributionSourceIsVocabulary() {
		return (Boolean) getField(DISTRIBUTION_SOURCE_IS_VOCABULARY);
	}

	public int getDeadLinks() {
		try {
			return Integer.parseInt(getField(DEAD_LINKS).toString());
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public String getDeadLinksAsString() {
		return String.valueOf(getField(DEAD_LINKS));
	}

	public void setDeadLinks(int invalidLinks) {
		addField(DEAD_LINKS, invalidLinks);
	}
	
}
