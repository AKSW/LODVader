package lodVader.mongodb.collections.gridFS;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;
import lodVader.mongodb.DBSuperClass2;

public class ObjectsBucket extends SuperBucket{
	
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
	
	public ObjectsBucket(BloomFilterI filter, String firstResource, String lastResource){
		this.filter = filter;
		this.firstResource = firstResource;
		this.lastResource = lastResource;
	}
	

	public TreeMap<String,ObjectsBucket> createAllBuckets(int distributionID) {
		
		TreeMap<String,ObjectsBucket> result = new TreeMap<String,ObjectsBucket>();

		// get collection
		GridFS gfs = new GridFS(DBSuperClass2.getDBInstance(), OBJECTS_BUCKET_COLLECTION_NAME);

		// create query
		BasicDBObject distribution = new BasicDBObject(DISTRIBUTION_ID, distributionID);

		// make query
		List<GridFSDBFile> buckets = gfs.find(distribution);

		for (GridFSDBFile f : buckets) {
			BloomFilterI filter = BloomFilterFactory.newBloomFilter();
			
			try {
				filter.readFrom(f.getInputStream()); 
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			result.put(f.get(FIRST_RESOURCE).toString(), new ObjectsBucket(filter, f.get(FIRST_RESOURCE).toString(), f.get(LAST_RESOURCE).toString()));
		}

		return result;
	}
	
}
