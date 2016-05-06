package lodVader.mongodb.collections;

import lodVader.mongodb.DBSuperClass2;

public class DatasetCoesionValues extends DBSuperClass2 {

	public DatasetCoesionValues() {
		super(COLLECTION_NAME);
		addPK(DATASET_ID);
		addMandatoryField(LINKS);
		addMandatoryField(LITERALS);
		addMandatoryField(COHESION);
		addMandatoryField(TRIPLES);
	}

	// Collection name
	public static final String COLLECTION_NAME = "Cohesion";

	// class properties
	public static final String TRIPLES = "triples";
	
	public static final String LINKS = "links";
	
	public static final String LITERALS = "literals";
	
	public static final String COHESION = "cohesion";

	public static final String IS_VOCAB = "isVocab";

	public static final String DATASET_ID = "dataset_id";	
	
	
	public int getTriples(){
		return Integer.parseInt(getField(TRIPLES).toString());
	}
	
	public Boolean getIsVocab(){
		return Boolean.parseBoolean(getField(IS_VOCAB).toString());
	}
	
	public int getLinks(){
		return Integer.parseInt(getField(LINKS).toString());
	}

	public int getLiterals(){
		return Integer.parseInt(getField(LITERALS).toString());
	}

	public String getDatasetID(){
		return (getField(DATASET_ID).toString());
	}
	
	public int getCohesion(){
		return Integer.parseInt(getField(COHESION).toString());
	}
	
	public void setTriples(int triples){
		addField(TRIPLES, triples);
	}

	public void setLinks(int links){
		addField(LINKS, links);
	}
	
	public void setIsVocab(boolean isVocab){
		addField(IS_VOCAB, isVocab);
	}

	public void setLiterals(int literals){
		addField(LITERALS, literals);
	}
	
	public void setCohesion(int cohesion){
		addField(COHESION, cohesion);
	}

	public void setDatasetID(String datasetId){
		addField(DATASET_ID, datasetId);
	}


}
