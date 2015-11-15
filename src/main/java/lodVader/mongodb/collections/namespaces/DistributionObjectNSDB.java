package lodVader.mongodb.collections.namespaces;

public class DistributionObjectNSDB extends SuperNS {

	public static final String NUMBER_OF_RESOURCES = "numberOfResources";
	public static String COLLECTION_NAME = "DistributionObjectNS";

	public DistributionObjectNSDB() {
		super.COLLECTION_NAME = DistributionObjectNSDB.COLLECTION_NAME;
	}

	public int getNumberOfResources() {
		return ((Number) getField(NUMBER_OF_RESOURCES)).intValue();
	}

	public void setNumberOfResources(int numberOfResources) {
		addField(NUMBER_OF_RESOURCES, numberOfResources);
	}

}
 