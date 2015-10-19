package lodVader.mongodb.collections;

import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.DBSuperClass;

public class LinksetDB extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "Linkset";

	// class properties
	public static final String DISTRIBUTION_TARGET = "distributionTarget";

	public static final String DISTRIBUTION_SOURCE = "distributionSource";

	public static final String DATASET_TARGET = "datasetTarget";

	public static final String DATASET_SOURCE = "datasetSource";

	public static final String INVALID_LINKS = "invalidLinks";

	public static final String PREDICATE_SIMILARITY = "predicateSimilarity";

	public static final String OWL_CLASS_SIMILARITY = "owlClassSimilarity";

	public static final String RDF_TYPE_SIMILARITY = "rdfTypeSimilarity";

	public static final String RDF_SUBCLASS_SIMILARITY = "rdfSubClassSimilarity";

	public static final String LINK_STRENGHT = "strength";

	public static final String LINK_NUMBER_LINKS = "links";

	private int distributionTarget;

	private int distributionSource;

	private int datasetTarget;

	private int datasetSource;

	private double predicateSimilarity = 0;

	private double owlClassSimilarity = 0;

	private double rdfTypeSimilarity = 0;

	private double rdfSubClassSimilarity = 0;

	private double strength;

	private int links = 0;

	private int invalidLinks = 0;
	
	public LinksetDB(String uri) {
		super(COLLECTION_NAME, uri);
		loadObject();
	}

	public LinksetDB(DBObject object) {
		super(COLLECTION_NAME, object);
		load(object);
	}
	
	
	
	
	protected void load(DBObject obj){

		if (obj != null) {

			distributionTarget = ((Number) obj
					.get(DISTRIBUTION_TARGET)).intValue();

			distributionSource =  ((Number) obj
					.get(DISTRIBUTION_SOURCE)).intValue();

			datasetSource =  ((Number) obj.get(DATASET_SOURCE)).intValue();

			datasetTarget =  ((Number) obj.get(DATASET_TARGET)).intValue();

			predicateSimilarity =  ((Number) obj.get(PREDICATE_SIMILARITY)).doubleValue();

			owlClassSimilarity =  ((Number) obj.get(OWL_CLASS_SIMILARITY)).doubleValue();

			rdfTypeSimilarity =  ((Number) obj.get(RDF_TYPE_SIMILARITY)).doubleValue();

			rdfSubClassSimilarity =  ((Number) obj.get(RDF_SUBCLASS_SIMILARITY)).doubleValue();

			strength =  ((Number) obj.get(LINK_STRENGHT)).doubleValue();

//			invalidLinks = Integer.valueOf(obj.get(INVALID_LINKS).toString());

			links = ((Number) obj.get(LINK_NUMBER_LINKS)).intValue();

			invalidLinks = ((Number) obj.get(INVALID_LINKS)).intValue();

		}
	}

	
	
	
	public boolean updateObject(boolean checkBeforeInsert) {

		// save object case it doens't exists
		try {
			// updating subjectsTarget on mongodb
			mongoDBObject.put(DISTRIBUTION_TARGET,
					distributionTarget);

			// updating objectsTarget on mongodb
			mongoDBObject.put(DISTRIBUTION_SOURCE,
					distributionSource);

			// updating subjectsTarget on mongodb
			mongoDBObject.put(DATASET_TARGET, datasetTarget);

			// updating objectsTarget on mongodb
			mongoDBObject.put(DATASET_SOURCE, datasetSource);

			// updating links on mongodb
			mongoDBObject.put(LINK_NUMBER_LINKS, links);

			// updating invalid links on mongodb
//			mongoDBObject.put(INVALID_LINKS, invalidLinks);
			

			// updating similarity on mongodb
			mongoDBObject.put(PREDICATE_SIMILARITY, predicateSimilarity);

			mongoDBObject.put(OWL_CLASS_SIMILARITY, owlClassSimilarity);

			mongoDBObject.put(RDF_SUBCLASS_SIMILARITY, rdfSubClassSimilarity);

			mongoDBObject.put(RDF_TYPE_SIMILARITY, rdfTypeSimilarity);

			mongoDBObject.put(INVALID_LINKS, invalidLinks);

			// updating link strength on mongodb
			mongoDBObject.put(LINK_STRENGHT, strength);

			insert(checkBeforeInsert);
		} catch (Exception e2) {
			// e2.printStackTrace();

			try {
				if (update())
					return true;
				else
					return false;
			} catch (LODVaderLODGeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		}
		return false;
	}

	protected boolean loadObject() {
		DBObject obj = search();

		if (obj != null) {

			distributionTarget = ((Number) obj
					.get(DISTRIBUTION_TARGET)).intValue();

			distributionSource =  ((Number) obj
					.get(DISTRIBUTION_SOURCE)).intValue();

			datasetSource =  ((Number) obj.get(DATASET_SOURCE)).intValue();

			datasetTarget =  ((Number) obj.get(DATASET_TARGET)).intValue();

			predicateSimilarity =  ((Number) obj.get(PREDICATE_SIMILARITY)).doubleValue();

			owlClassSimilarity =  ((Number) obj.get(OWL_CLASS_SIMILARITY)).doubleValue();

			rdfSubClassSimilarity =  ((Number) obj.get(RDF_SUBCLASS_SIMILARITY)).doubleValue();

			rdfTypeSimilarity =  ((Number) obj.get(RDF_TYPE_SIMILARITY)).doubleValue();

			strength =  ((Number) obj.get(LINK_STRENGHT)).doubleValue();

//			invalidLinks = Integer.valueOf(obj.get(INVALID_LINKS).toString());

			links = ((Number) obj.get(LINK_NUMBER_LINKS)).intValue();

			invalidLinks= ((Number) obj.get(INVALID_LINKS)).intValue();

			return true;
		}
		return false;
	}

	public int getDistributionTarget() {
		return distributionTarget;
	}

	public void setDistributionTarget(int subjectsDistributionTarget) {
		this.distributionTarget = subjectsDistributionTarget;
	}

	public int getDistributionSource() {
		return distributionSource;
	}

	public void setDistributionSource(int objectsDistributionTarget) {
		this.distributionSource = objectsDistributionTarget;
	}

	public int getDatasetTarget() {
		return datasetTarget;
	}

	public void setDatasetTarget(int subjectsDatasetTarget) {
		this.datasetTarget = subjectsDatasetTarget;
	}

	public int getDatasetSource() {
		return datasetSource;
	}

	public void setDatasetSource(int objectsDatasetTarget) {
		this.datasetSource = objectsDatasetTarget;
	}

	public int getLinks() {
		return links;
	}
	
	public String getLinksAsString(){
		return String.valueOf(links);
	}

	public void setLinks(int links) {
		this.links = links;
	}

	public int getInvalidLinks() {
		return invalidLinks;
	}
	
	public String getInvalidLinksAsString() {
		return String.valueOf(invalidLinks);
	}

	public void setInvalidLinks(int invalidLinks) {
		this.invalidLinks = invalidLinks;
	}

	public double getPredicateSimilarity() {
		return predicateSimilarity;
	}
	
	public String getPredicatesSimilarityAsString() {
		return String.valueOf(predicateSimilarity);
	}

	public void setPredicateSimilarity(double similarity) {
		this.predicateSimilarity = similarity;
	}

	
	
	public double getOwlClassSimilarity() {
		return owlClassSimilarity;
	}

	public void setOwlClassSimilarity(double owlClassSimilarity) {
		this.owlClassSimilarity = owlClassSimilarity;
	}

	public double getRdfTypeSimilarity() {
		return rdfTypeSimilarity;
	}

	public void setRdfTypeSimilarity(double rdfTypeSimilarity) {
		this.rdfTypeSimilarity = rdfTypeSimilarity;
	}

	public double getRdfSubClassSimilarity() {
		return rdfSubClassSimilarity;
	}

	public void setRdfSubClassSimilarity(double rdfSubClassSimilarity) {
		this.rdfSubClassSimilarity = rdfSubClassSimilarity;
	}

	public double getStrength() {
		return strength;
	}

	public String getStrengthAsString() {
		return String.valueOf(strength);
	}

	public void setStrength(double strength) {
		this.strength = strength;
	}

	
	
	
}
