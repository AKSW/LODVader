package lodVader.mongodb.collections.namespaces;

import java.util.Set;

import com.mongodb.BasicDBObject;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DistributionDB;

public class SuperNS extends DBSuperClass2{

	public SuperNS(String collectionName) {
		super(collectionName);
		setParameters();
	}
	
	private void setParameters(){
		addMandatoryField(DATASET_ID);
		addMandatoryField(DISTRIBUTION_ID);
		addMandatoryField(NS);
	}
	
	// class properties
	public static final String DISTRIBUTION_ID = "distributionID";

	public static final String DATASET_ID = "datasetID";

	public static final String NS = "ns";

	public int getDistributionID() {
		return ((Number) mongoDBObject.get(DISTRIBUTION_ID)).intValue();
	}

	public void setDistributionID(int distributionID) {
		mongoDBObject.put(DISTRIBUTION_ID, distributionID);
	}

	public int getDatasetID() {
		return ((Number) mongoDBObject.get(DATASET_ID)).intValue();
	}

	public void setDatasetID(int datasetID) {
		mongoDBObject.put(DATASET_ID, datasetID);
	}

	public String getNS() {
		return getField(NS).toString();
	}

	public void setNS(String ns) {
		addField(NS, ns);
	}
	
	public void bulkSave(Set<String> nsSet, DistributionDB distribution){
		getCollection().remove(new BasicDBObject(DISTRIBUTION_ID, distribution.getLODVaderID()));
		for (String s : nsSet) {
			try {
				mongoDBObject = new BasicDBObject();
				setDistributionID(distribution.getLODVaderID());
				setNS(s);
				setDatasetID(distribution.getTopDatasetID());
				try {
					update(true);
				} catch (LODVaderObjectAlreadyExistsException | LODVaderNoPKFoundException e) {
					e.printStackTrace();
				}
			} catch (LODVaderMissingPropertiesException e) {
				e.printStackTrace();
			}
		}
	}

}
