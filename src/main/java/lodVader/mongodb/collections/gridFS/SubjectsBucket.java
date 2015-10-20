package lodVader.mongodb.collections.gridFS;

import java.util.TreeSet;

public class SubjectsBucket extends SuperBucket{
	
	
	public SubjectsBucket() {
		this.COLLECTION_NAME = "SubjectsBucket";
	}
	
	public SubjectsBucket(TreeSet<String> resources, int distributionID) {
		this.COLLECTION_NAME = "SubjectsBucket";
		this.resources = resources;
		this.distributionID = distributionID;
	}
	
	public SubjectsBucket(String resource, int distributionID) {
		this.COLLECTION_NAME = "SubjectsBucket";
		this.resource = resource;
		this.distributionID = distributionID;
	}
	
}
