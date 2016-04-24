package lodVader.mongodb.collections.gridFS;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.hash.BloomFilter;
import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.DBSuperClass2;

public class SubjectsBucket extends SuperBucket {

	public static String SUBJECTS_BUCKET_COLLECTION_NAME = "SubjectsBucket";

	public SubjectsBucket() {
		this.COLLECTION_NAME = SUBJECTS_BUCKET_COLLECTION_NAME;
	}

	public SubjectsBucket(String resource, int distributionID) {
		this.COLLECTION_NAME = SUBJECTS_BUCKET_COLLECTION_NAME;
		this.resource = resource;
		this.distributionID = distributionID;
	}

	public SubjectsBucket(File resourcesFile, int distributionID) {
		this.COLLECTION_NAME = SUBJECTS_BUCKET_COLLECTION_NAME;
		this.resourcesFile = resourcesFile;
		this.distributionID = distributionID;
	}

	public SubjectsBucket(TreeSet<String> resources, int distributionID) {
		this.COLLECTION_NAME = SUBJECTS_BUCKET_COLLECTION_NAME;
		this.resources = resources;
		this.distributionID = distributionID;
	}

	public SubjectsBucket(GoogleBloomFilter filter, String firstResource, String lastResource) {
		this.filter = filter;
		this.firstResource = firstResource;
		this.lastResource = lastResource;
	}

	public  TreeMap<String,SubjectsBucket> createAllBuckets(int distributionID) {
		
		TreeMap<String,SubjectsBucket> result = new  TreeMap<String,SubjectsBucket>();

		// get collection
		GridFS gfs = new GridFS(DBSuperClass2.getDBInstance(), SUBJECTS_BUCKET_COLLECTION_NAME);

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
			result.put(f.get(FIRST_RESOURCE).toString(),new SubjectsBucket(filter, f.get(FIRST_RESOURCE).toString(), f.get(LAST_RESOURCE).toString()));
		}

		return result;
	}

}
