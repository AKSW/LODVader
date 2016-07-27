package lodVader.mongodb.collections;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.toplinks.TopValidLinks;

public class LinksetDB extends DBSuperClass2 {

	public LinksetDB() {
		super(COLLECTION_NAME);
		initializeVariables();

	}

	// Collection name
	public static final String COLLECTION_NAME = "Linkset";

	// class properties
	public static final String LINKSET_ID = "linksetId";

	public static final String DISTRIBUTION_TARGET = "distributionTarget";

	public static final String DISTRIBUTION_SOURCE = "distributionSource";

	public static final String DISTRIBUTION_TARGET_IS_VOCABULARY = "distributionTargetIsVocabulary";

	public static final String DISTRIBUTION_SOURCE_IS_VOCABULARY = "distributionSourceIsVocabulary";

	public static final String DATASET_TARGET = "datasetTarget";

	public static final String DATASET_SOURCE = "datasetSource";

	public static final String INVALID_LINKS = "invalidLinks";

	public static final String PREDICATE_SIMILARITY = "predicateSimilarity";

	public static final String OWL_CLASS_SIMILARITY = "owlClassSimilarity";

	public static final String RDF_TYPE_SIMILARITY = "rdfTypeSimilarity";

	public static final String RDF_SUBCLASS_SIMILARITY = "rdfSubClassSimilarity";

	public static final String LINK_STRENGHT = "strength";

	public static final String LINK_NUMBER_LINKS = "links";

	public LinksetDB(DBObject object) {
		super(COLLECTION_NAME);
		initializeVariables();
		mongoDBObject = object;
	}

	public LinksetDB(String id) {
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
		addMandatoryField(LINK_NUMBER_LINKS);
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
			return Integer.parseInt(getField(LINK_NUMBER_LINKS).toString());
		} catch (NullPointerException e) {
			setLinks(0);
			return 0;
		}
	}

	public String getLinksAsString() {
		return String.valueOf(getField(LINK_NUMBER_LINKS));
	}

	public void setLinks(int links) {
		addField(LINK_NUMBER_LINKS, links);
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

	public int getInvalidLinks() {
		try {
			return Integer.parseInt(getField(INVALID_LINKS).toString());
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public String getInvalidLinksAsString() {
		return String.valueOf(getField(INVALID_LINKS));
	}

	public void setInvalidLinks(int invalidLinks) {
		addField(INVALID_LINKS, invalidLinks);
	}

	public double getPredicateSimilarity() {
		try {
			return Double.parseDouble(getField(PREDICATE_SIMILARITY).toString());
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public String getPredicatesSimilarityAsString() {
		return String.valueOf(getField(PREDICATE_SIMILARITY));
	}

	public void setPredicateSimilarity(double similarity) {
		addField(PREDICATE_SIMILARITY, similarity);
	}

	public double getOwlClassSimilarity() {
		try {
			return Double.parseDouble(getField(OWL_CLASS_SIMILARITY).toString());
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public void setOwlClassSimilarity(double owlClassSimilarity) {
		addField(OWL_CLASS_SIMILARITY, owlClassSimilarity);
	}

	public double getRdfTypeSimilarity() {
		try {
			return Double.parseDouble(getField(RDF_TYPE_SIMILARITY).toString());
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public void setRdfTypeSimilarity(double rdfTypeSimilarity) {
		addField(RDF_TYPE_SIMILARITY, rdfTypeSimilarity);
	}

	public double getRdfSubClassSimilarity() {
		try {
			return Double.parseDouble(getField(RDF_SUBCLASS_SIMILARITY).toString());
		} catch (NullPointerException e) {
			setRdfSubClassSimilarity(0);
			return 0;
		}

	}

	public void setRdfSubClassSimilarity(double rdfSubClassSimilarity) {
		addField(RDF_SUBCLASS_SIMILARITY, rdfSubClassSimilarity);
	}

	public double getStrength() {
		try {
			return Double.parseDouble(getField(LINK_STRENGHT).toString());
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public String getStrengthAsString() {
		return String.valueOf(getField(LINK_STRENGHT));
	}

	public void setStrength(double strength) {
		addField(LINK_STRENGHT, strength);
	}

	public void removeAllLinks(int distributionID) {
		BasicDBObject query = new BasicDBObject(LinksetDB.DISTRIBUTION_SOURCE, distributionID);
		BasicDBObject query2 = new BasicDBObject(LinksetDB.DISTRIBUTION_TARGET, distributionID);
		BasicDBList or = new BasicDBList();
		or.add(query);
		or.add(query2);
		DBSuperClass2.getDBInstance().getCollection(LinksetDB.COLLECTION_NAME).remove(new BasicDBObject("$or", or));

		query = new BasicDBObject(TopValidLinks.SOURCE_DISTRIBUTION_ID, distributionID);
		query2 = new BasicDBObject(TopValidLinks.TARGET_DISTRIBUTION_ID, distributionID);
		or = new BasicDBList();
		or.add(query);
		or.add(query2);

		DBSuperClass2.getDBInstance().getCollection(TopValidLinks.COLLECTION_NAME).remove(new BasicDBObject("$or", or));

	}

}
