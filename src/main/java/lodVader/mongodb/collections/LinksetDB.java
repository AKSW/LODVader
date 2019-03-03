package lodVader.mongodb.collections;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.toplinks.TopValidLinks;

public class LinksetDB {
	
	public DBSuperClass2 db;

	public LinksetDB(DBSuperClass2 db) {
		this.db = db;
		this.db.COLLECTION_NAME = COLLECTION_NAME;
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

	public void init(DBObject object) {
		initializeVariables();
		db.mongoDBObject = object;
	}

	public void init(String id) {
		initializeVariables();
		setLinksetID(id);
		db.find(true);
	}

	private void initializeVariables() {
		db.addPK(LINKSET_ID);
		db.addMandatoryField(LINKSET_ID);
		db.addMandatoryField(DISTRIBUTION_SOURCE);
		db.addMandatoryField(DISTRIBUTION_TARGET);
		db.addMandatoryField(DATASET_SOURCE);
		db.addMandatoryField(DATASET_TARGET);
		db.addMandatoryField(LINK_NUMBER_LINKS);
		// addMandatoryField(LINK_STRENGHT);
		// addMandatoryField(INVALID_LINKS);
		// addMandatoryField(PREDICATE_SIMILARITY);
		// addMandatoryField(OWL_CLASS_SIMILARITY);
		// addMandatoryField(RDF_SUBCLASS_SIMILARITY);
		// addMandatoryField(RDF_TYPE_SIMILARITY);
	}

	public int getDistributionTarget() {
		return Integer.parseInt(db.getField(DISTRIBUTION_TARGET).toString());
	}

	public void setDistributionTarget(int distributionTarget) {
		db.addField(DISTRIBUTION_TARGET, distributionTarget);
	}

	public String getLinksetID() {
		return db.getField(LINKSET_ID).toString();
	}

	public void setLinksetID(String id) {
		db.addField(LINKSET_ID, id);
	}

	public int getDistributionSource() {
		return Integer.parseInt(db.getField(DISTRIBUTION_SOURCE).toString());
	}

	public void setDistributionSource(int distributionSource) {
		db.addField(DISTRIBUTION_SOURCE, distributionSource);
	}

	public int getDatasetTarget() {
		return Integer.parseInt(db.getField(DATASET_TARGET).toString());
	}

	public void setDatasetTarget(int datasetTarget) {
		db.addField(DATASET_TARGET, datasetTarget);
	}

	public int getDatasetSource() {
		return Integer.parseInt(db.getField(DATASET_SOURCE).toString());
	}

	public void setDatasetSource(int datasetSource) {
		db.addField(DATASET_SOURCE, datasetSource);
	}

	public int getLinks() {
		try {
			return Integer.parseInt(db.getField(LINK_NUMBER_LINKS).toString());
		} catch (NullPointerException e) {
			setLinks(0);
			return 0;
		}
	}

	public String getLinksAsString() {
		return String.valueOf(db.getField(LINK_NUMBER_LINKS));
	}

	public void setLinks(int links) {
		db.addField(LINK_NUMBER_LINKS, links);
	}

	public void setDistributionTargetIsVocabulary(Boolean isVocabulary) {
		db.addField(DISTRIBUTION_TARGET_IS_VOCABULARY, isVocabulary);
	}

	public void setDistributionSourceIsVocabulary(Boolean isVocabulary) {
		db.addField(DISTRIBUTION_SOURCE_IS_VOCABULARY, isVocabulary);
	}

	public Boolean getDistributionTargetIsVocabulary() {
		return (Boolean) db.getField(DISTRIBUTION_TARGET_IS_VOCABULARY);
	}

	public Boolean getDistributionSourceIsVocabulary() {
		return (Boolean) db.getField(DISTRIBUTION_SOURCE_IS_VOCABULARY);
	}

	public int getInvalidLinks() {
		try {
			return Integer.parseInt(db.getField(INVALID_LINKS).toString());
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public String getInvalidLinksAsString() {
		return String.valueOf(db.getField(INVALID_LINKS));
	}

	public void setInvalidLinks(int invalidLinks) {
		db.addField(INVALID_LINKS, invalidLinks);
	}

	public double getPredicateSimilarity() {
		try {
			return Double.parseDouble(db.getField(PREDICATE_SIMILARITY).toString());
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public String getPredicatesSimilarityAsString() {
		return String.valueOf(db.getField(PREDICATE_SIMILARITY));
	}

	public void setPredicateSimilarity(double similarity) {
		db.addField(PREDICATE_SIMILARITY, similarity);
	}

	public double getOwlClassSimilarity() {
		try {
			return Double.parseDouble(db.getField(OWL_CLASS_SIMILARITY).toString());
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public void setOwlClassSimilarity(double owlClassSimilarity) {
		db.addField(OWL_CLASS_SIMILARITY, owlClassSimilarity);
	}

	public double getRdfTypeSimilarity() {
		try {
			return Double.parseDouble(db.getField(RDF_TYPE_SIMILARITY).toString());
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public void setRdfTypeSimilarity(double rdfTypeSimilarity) {
		db.addField(RDF_TYPE_SIMILARITY, rdfTypeSimilarity);
	}

	public double getRdfSubClassSimilarity() {
		try {
			return Double.parseDouble(db.getField(RDF_SUBCLASS_SIMILARITY).toString());
		} catch (NullPointerException e) {
			setRdfSubClassSimilarity(0);
			return 0;
		}

	}

	public void setRdfSubClassSimilarity(double rdfSubClassSimilarity) {
		db.addField(RDF_SUBCLASS_SIMILARITY, rdfSubClassSimilarity);
	}

	public double getStrength() {
		try {
			return Double.parseDouble(db.getField(LINK_STRENGHT).toString());
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public String getStrengthAsString() {
		return String.valueOf(db.getField(LINK_STRENGHT));
	}

	public void setStrength(double strength) {
		db.addField(LINK_STRENGHT, strength);
	}

	public void removeAllLinks(int distributionID) {
		BasicDBObject query = new BasicDBObject(LinksetDB.DISTRIBUTION_SOURCE, distributionID);
		BasicDBObject query2 = new BasicDBObject(LinksetDB.DISTRIBUTION_TARGET, distributionID);
		BasicDBList or = new BasicDBList();
		or.add(query);
		or.add(query2);
		db.getCollection(LinksetDB.COLLECTION_NAME).remove(new BasicDBObject("$or", or));

		query = new BasicDBObject(TopValidLinks.SOURCE_DISTRIBUTION_ID, distributionID);
		query2 = new BasicDBObject(TopValidLinks.TARGET_DISTRIBUTION_ID, distributionID);
		or = new BasicDBList();
		or.add(query);
		or.add(query2);

		db.getCollection(TopValidLinks.COLLECTION_NAME).remove(new BasicDBObject("$or", or));

	}

}
