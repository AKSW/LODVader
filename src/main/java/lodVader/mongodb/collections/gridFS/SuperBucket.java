package lodVader.mongodb.collections.gridFS;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.externalsorting.ExternalSort;
import com.google.common.hash.BloomFilter;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import lodVader.LODVaderProperties;
import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.DBSuperClass2;

public class SuperBucket {

	final static Logger logger = LoggerFactory.getLogger(SuperBucket.class);

//	static double time = 0;

	int chunkSize = LODVaderProperties.MAX_CHUNK_SIZE;

	double fpp = 0.0000001;

	public String COLLECTION_NAME = null;

	public static String FIRST_RESOURCE = "firstResource";

	public static String LAST_RESOURCE = "lastResource";

	public static String DISTRIBUTION_ID = "distributionID";

	public String firstResource = "";

	public String lastResource = "";

	public String resource;

	public GoogleBloomFilter filter = null;

	int distributionID;

	public TreeSet<String> resources;

	protected File resourcesFile = null;

	protected File resourcesFileSorted = null;

	public SuperBucket() {
	}


	public SuperBucket(File resourcesFile, int distributionID) {
		this.resourcesFile = resourcesFile;
		this.distributionID = distributionID;
	}

	public SuperBucket(GoogleBloomFilter filter, String firstResource, String lastResource) {
		this.filter = filter;
		this.firstResource = firstResource;
		this.lastResource = lastResource;
	}

//	@SuppressWarnings("unchecked")
	public void makeBucket() {

		// first we have to remove the old BFs for this distribution
		removeBFs(distributionID);

		// chunk
		ArrayList<String> chunk = new ArrayList<String>();

		try {
			// take the chunks of chunkSize resources
			if (resources != null) {
				for (String resource : resources) {
					chunk.add(resource);
					if (chunk.size() == chunkSize) {
						saveChunk((ArrayList<String>) chunk.clone());
						chunk = new ArrayList<String>();
					}
				}
			} else if (resourcesFile != null) {
				String resource;
				// use external sort tool

				resourcesFileSorted = new File(LODVaderProperties.TMP_FOLDER+"sorted_tmp");
				
				File tmpFolder = new File(LODVaderProperties.TMP_FOLDER);
				
				Comparator<String> comparator = ExternalSort.defaultcomparator;
				List<File> l = ExternalSort.sortInBatch(resourcesFile, comparator, ExternalSort.DEFAULTMAXTEMPFILES, Charset.defaultCharset(), 
						tmpFolder, true,
						0, false);
				logger.info("created " + l.size() + " tmp files");
				ExternalSort.mergeSortedFiles(l, resourcesFileSorted, comparator, Charset.defaultCharset(), true, false, false);

				BufferedReader f = new BufferedReader(new FileReader(resourcesFileSorted));
				// for(String resource: resources){
				while ((resource = f.readLine()) != null) {
					chunk.add(resource);
					if (chunk.size() == chunkSize) {
						saveChunk((ArrayList<String>) chunk.clone());
						chunk = new ArrayList<String>();
					}
				}
				resourcesFileSorted.delete();
			} else {
				throw new LODVaderLODGeneralException("No items to load in BF!");
			}
			if (chunk.size() > 0)
				saveChunk((ArrayList<String>) chunk.clone());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void removeBFs(int distributionID) {
		GridFS gridFS = new GridFS(DBSuperClass.getInstance(), COLLECTION_NAME);
		gridFS.remove(new BasicDBObject(DISTRIBUTION_ID, distributionID));
	}

	private void saveChunk(final ArrayList<String> chunk) {
		final int distributionID = this.distributionID;
//		Thread t = new Thread() {
//			public void run() {
				makeBloomFilter(chunk, distributionID);
//			};
//		};
//		t.start();
//		try {
//			t.join();
//		} catch (InterruptedException e) {
//			 TODO Auto-generated catch block
//			e.printStackTrace();
//		}

	}

	private void makeBloomFilter(ArrayList<String> chunk, int distributionID) {

		final String firstResource = chunk.get(0);
		final String lastResource = chunk.get(chunk.size() - 1);
		int chunkSize = chunk.size();
		if (chunkSize < 5000)
			chunkSize = 5000;
		GoogleBloomFilter filter = new GoogleBloomFilter(chunkSize, fpp);
		for (String resource : chunk) {
			filter.add(resource);
		}

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		try {
			filter.filter.writeTo(out);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		GridFS gfs = new GridFS(DBSuperClass2.getDBInstance(), COLLECTION_NAME);
		GridFSInputFile gfsFile;
		try {
			gfsFile = gfs.createFile(new ByteArrayInputStream(out.toByteArray()));
			gfsFile.put(FIRST_RESOURCE, firstResource);
			gfsFile.put(LAST_RESOURCE, lastResource);
			gfsFile.put(DISTRIBUTION_ID, distributionID);
			gfsFile.save();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// @Test
	// public void query(){
	public boolean query(int distributionID) {

		GridFS gfsFile = new GridFS(DBSuperClass2.getDBInstance(), COLLECTION_NAME);

		boolean result = false;

		BasicDBObject firstResource = new BasicDBObject(FIRST_RESOURCE, new BasicDBObject("$lte", resource));
		BasicDBObject lastResource = new BasicDBObject(LAST_RESOURCE, new BasicDBObject("$gte", resource));
		BasicDBObject distribution = new BasicDBObject(DISTRIBUTION_ID, distributionID);

		BasicDBList and = new BasicDBList();
		and.add(firstResource);
		and.add(lastResource);
		and.add(distribution);

		GridFSDBFile file = gfsFile.findOne(new BasicDBObject("$and", and));

		// Timer t = new Timer();
		// t.startTimer();
		GoogleBloomFilter filter = new GoogleBloomFilter();
		if (file != null)
			try {
				filter.filter = BloomFilter.readFrom(file.getInputStream(), filter.funnel);
				result = filter.filter.mightContain(resource.getBytes());
			
			} catch (IOException e) {
				e.printStackTrace();
			}
		// time=time+Double.parseDouble(t.stopTimer());
		// System.out.println(time);

		return result;
	}

	public GoogleBloomFilter getFilter() {

		GridFS gfs = new GridFS(DBSuperClass.getInstance(), COLLECTION_NAME);

		BasicDBObject firstResource = new BasicDBObject(FIRST_RESOURCE, new BasicDBObject("$lte", resource));
		BasicDBObject lastResource = new BasicDBObject(LAST_RESOURCE, new BasicDBObject("$gte", resource));
		BasicDBObject distribution = new BasicDBObject(DISTRIBUTION_ID, distributionID);

		BasicDBList and = new BasicDBList();
		and.add(firstResource);
		and.add(lastResource);
		and.add(distribution);

		// System.out.println(new BasicDBObject("$and",and));

		GridFSDBFile file = gfs.findOne(new BasicDBObject("$and", and));

		GoogleBloomFilter filter = null;
		if (file != null)
			try {
				System.out.println(file.get(FIRST_RESOURCE));
				filter = new GoogleBloomFilter();
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
