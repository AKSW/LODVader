package lodVader.mongodb.collections.gridFS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import com.google.common.hash.BloomFilter;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.DBSuperClass;

public abstract class SuperBucket {

	int chunkSize = 25000;
//	int chunkSize = 54480;
	
	double fpp = 0.00000001;
	public String COLLECTION_NAME = null;

	String FIRST_RESOURCE = "firstResource";
	
	String LAST_RESOURCE = "lastResource";
	
	String DISTRIBUTION_ID = "distributionID";
	
	String firstResource = null;

	String lastResource = null;
	
	String resource = null;
	
	GoogleBloomFilter filter = null;
	
	int distributionID;

	TreeSet<String> resources;
	
	public SuperBucket() {
	}
	
	public SuperBucket(TreeSet<String> resources, int distributionID) {
		this.resources = resources;
		this.distributionID = distributionID;
	}
	
	
	public void makeBucket(){
		
		// first we have to remove the old BFs for this distribution
		
		removeBFs(distributionID);
		
		// chunk
		ArrayList<String> chunk = new ArrayList<String>();
		
		// take the chunks of chunkSize resources
		for(String resource: resources){
			chunk.add(resource);
			
			if(chunk.size() == chunkSize){
				saveChunk((ArrayList<String>) chunk.clone());
				chunk = new ArrayList<String>();
			}
		}
		
		if(chunk.size()>0)
			saveChunk((ArrayList<String>) chunk.clone());
	}
	
	
	private void removeBFs(int distributionID){
		GridFS gridFS = new GridFS(DBSuperClass.getInstance(), COLLECTION_NAME);
		gridFS.remove(new BasicDBObject(DISTRIBUTION_ID, distributionID));
	}
	
	
	private void saveChunk(final ArrayList<String> chunk){	
		final int distributionID = this.distributionID;
		
		Thread t = new Thread(){
			public void run() {
				makeBloomFilter(chunk, distributionID);						
			};
		};
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	private void makeBloomFilter(ArrayList<String> chunk, int distributionID){
		
		final String firstResource = chunk.get(0);
		final String lastResource = chunk.get(chunk.size() -1);
		
		GoogleBloomFilter filter = new GoogleBloomFilter(chunkSize, fpp);
		for(String resource: chunk){
			filter.add(resource);
		}
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		
		try {
			filter.filter.writeTo(out);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
		
		String newFileName = "nome";
		GridFS gfsPhoto = new GridFS( DBSuperClass.getInstance(), COLLECTION_NAME);
		GridFSInputFile gfsFile;
		try {
			gfsFile = gfsPhoto.createFile(new ByteArrayInputStream(out.toByteArray()));
			gfsFile.setFilename(newFileName);
			gfsFile.put(FIRST_RESOURCE, firstResource);
			gfsFile.put(LAST_RESOURCE, lastResource);
			gfsFile.put(DISTRIBUTION_ID, distributionID);
			gfsFile.save();			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
	}
	
	public boolean query(String resource, int distributionID){
		
		GridFS gfsPhoto = new GridFS(DBSuperClass.getInstance(), COLLECTION_NAME);
		
		boolean result = false;
		
		BasicDBObject firstResource = new BasicDBObject(FIRST_RESOURCE, new BasicDBObject("$lte", resource));
		BasicDBObject lastResource = new BasicDBObject(LAST_RESOURCE, new BasicDBObject("$gte", resource));
		BasicDBObject distribution = new BasicDBObject(DISTRIBUTION_ID, distributionID);
		
		BasicDBList and = new BasicDBList();
		and.add(firstResource);
		and.add(lastResource);
		and.add(distribution);
		
		GridFSDBFile file = gfsPhoto.findOne(new BasicDBObject("$and",and));
		
		GoogleBloomFilter filter = new GoogleBloomFilter();
		try {
			filter.filter = BloomFilter.readFrom(file.getInputStream(), filter.funnel);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		result = filter.filter.mightContain(resource.getBytes());
		
		return result;
	}
	
	public GoogleBloomFilter getFilter(){
		
		GridFS gfsPhoto = new GridFS(DBSuperClass.getInstance(), COLLECTION_NAME);
		
		BasicDBObject firstResource = new BasicDBObject(FIRST_RESOURCE, new BasicDBObject("$lte", resource));
		BasicDBObject lastResource = new BasicDBObject(LAST_RESOURCE, new BasicDBObject("$gte", resource));
		BasicDBObject distribution = new BasicDBObject(DISTRIBUTION_ID, distributionID);
		
		BasicDBList and = new BasicDBList();
		and.add(firstResource);
		and.add(lastResource);
		and.add(distribution);
		
		GridFSDBFile file = gfsPhoto.findOne(new BasicDBObject("$and",and));
		
		GoogleBloomFilter filter = new GoogleBloomFilter();
		try {
			filter.filter = BloomFilter.readFrom(file.getInputStream(), filter.funnel);
			this.lastResource = file.get(LAST_RESOURCE).toString();
			this.firstResource = file.get(FIRST_RESOURCE).toString();
			this.filter = filter;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return filter;
	}
	
}
