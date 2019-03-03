package lodVader.mongodb.collections.namespaces;

import lodVader.mongodb.DBSuperClass2;

public class DistributionSubjectNS0DB extends SuperNS {

	public static String COLLECTION_NAME = "DistributionSubjectNS0";
	
	public DistributionSubjectNS0DB(DBSuperClass2 db){
		super(db);
		init(COLLECTION_NAME);
	}
	
}
