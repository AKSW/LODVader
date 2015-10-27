package lodVader.mongodb.collections.gridFS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.common.hash.BloomFilter;
import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.DBSuperClass;

public class SubjectsBucket extends SuperBucket {

	public static String SUBJECTS_BUCKET_COLLECTION_NAME = "SubjectsBucket";

	public SubjectsBucket() {
		this.COLLECTION_NAME = SUBJECTS_BUCKET_COLLECTION_NAME;
	}

	public SubjectsBucket(ArrayList<String> resources, int distributionID) {
		this.COLLECTION_NAME = SUBJECTS_BUCKET_COLLECTION_NAME;
		this.resources = resources;
		this.distributionID = distributionID;
	}

	public SubjectsBucket(String resource, int distributionID) {
		this.COLLECTION_NAME = SUBJECTS_BUCKET_COLLECTION_NAME;
		this.resource = resource;
		this.distributionID = distributionID;
	}
	
	public SubjectsBucket(GoogleBloomFilter filter, String firstResource, String lastResource){
		this.filter = filter;
		this.firstResource = firstResource;
		this.lastResource = lastResource;
	}
	
	@Test
	public void oi(){
		resource = "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#RFC5147String";
		System.out.println(query(60));
		
	}

	public ArrayList<SubjectsBucket> createAllBuckets(int distributionID) {
		
		ArrayList<SubjectsBucket> result = new ArrayList<SubjectsBucket>();

		// get collection
		GridFS gfs = new GridFS(DBSuperClass.getInstance(), SUBJECTS_BUCKET_COLLECTION_NAME);

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
			result.add(new SubjectsBucket(filter, f.get(FIRST_RESOURCE).toString(), f.get(LAST_RESOURCE).toString()));
		}

		return result;
	}

}
