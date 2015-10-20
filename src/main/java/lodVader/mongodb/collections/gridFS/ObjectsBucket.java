package lodVader.mongodb.collections.gridFS;

import java.util.TreeSet;

public class ObjectsBucket  extends SuperBucket{
	
	
	public ObjectsBucket() {
		this.COLLECTION_NAME = "ObjectsBucket";
	}
	
	public ObjectsBucket(TreeSet<String> resources, int distributionID) {
		this.COLLECTION_NAME = "ObjectsBucket";		
		this.resources = resources;
		this.distributionID = distributionID;
	}
	
	public ObjectsBucket(String resource, int distributionID) {
		this.COLLECTION_NAME = "ObjectsBucket";		
		this.resource = resource;
		this.distributionID = distributionID;
	}
	
}
