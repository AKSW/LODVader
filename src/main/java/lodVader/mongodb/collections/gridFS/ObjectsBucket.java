package lodVader.mongodb.collections.gridFS;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import com.google.common.hash.BloomFilter;
import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.DBSuperClass;

public class ObjectsBucket  extends SuperBucket{
	
	public static String OBJECTS_BUCKET_COLLECTION_NAME = "ObjectsBucket";
	
	public ObjectsBucket() {
		this.COLLECTION_NAME = OBJECTS_BUCKET_COLLECTION_NAME;
	}
	
	public ObjectsBucket(File resourceFile, int distributionID) {
		this.COLLECTION_NAME = OBJECTS_BUCKET_COLLECTION_NAME;		
		this.resourcesFile = resourceFile;
		this.distributionID = distributionID;
	}
	
	public ObjectsBucket(String resource, int distributionID) {
		this.COLLECTION_NAME = OBJECTS_BUCKET_COLLECTION_NAME;		
		this.resource = resource;
		this.distributionID = distributionID;
	}
	
	public ObjectsBucket(TreeSet<String> resources, int distributionID) {
		this.COLLECTION_NAME = OBJECTS_BUCKET_COLLECTION_NAME;		
		this.resources = resources;
		this.distributionID = distributionID;
	}
	
	public ObjectsBucket(GoogleBloomFilter filter, String firstResource, String lastResource){
		this.filter = filter;
		this.firstResource = firstResource;
		this.lastResource = lastResource;
	}
	

	public ArrayList<ObjectsBucket> createAllBuckets(int distributionID) {
		
		ArrayList<ObjectsBucket> result = new ArrayList<ObjectsBucket>();

		// get collection
		GridFS gfs = new GridFS(DBSuperClass.getInstance(), OBJECTS_BUCKET_COLLECTION_NAME);

		// create query
		BasicDBObject distribution = new BasicDBObject(DISTRIBUTION_ID, distributionID);

		// make query
		List<GridFSDBFile> buckets = gfs.find(distribution);

		for (GridFSDBFile f : buckets) {
			GoogleBloomFilter filter = new GoogleBloomFilter();
			try {
				filter.filter = BloomFilter.readFrom(f.getInputStream(), filter.funnel);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			result.add(new ObjectsBucket(filter, f.get(FIRST_RESOURCE).toString(), f.get(LAST_RESOURCE).toString()));
		}

		return result;
	}
	
}
