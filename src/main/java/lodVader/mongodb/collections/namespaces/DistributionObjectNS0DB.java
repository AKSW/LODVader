package lodVader.mongodb.collections.namespaces;

import lodVader.mongodb.DBSuperClass2;

public class DistributionObjectNS0DB extends SuperNS {
	
	public static String COLLECTION_NAME = "DistributionObjectNS0";
	
	
	public DistributionObjectNS0DB(DBSuperClass2 db) {		
		super(db);
		super.init(COLLECTION_NAME);

	}	

}
