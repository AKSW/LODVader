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
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import lodVader.LODVaderProperties;
import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;
import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.linksets.DistributionBloomFilterContainer;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.queries.DatasetQueries;

public class SuperBucket extends Thread {

	final static Logger logger = LoggerFactory.getLogger(SuperBucket.class);

	// static double time = 0;

	int chunkSize = LODVaderProperties.MAX_CHUNK_SIZE;

	double fpp = 0.000001;

	public String COLLECTION_NAME = null;

	public static String FIRST_RESOURCE = "firstResource";

	public static String LAST_RESOURCE = "lastResource";

	public static String DISTRIBUTION_ID = "distributionID";

	public String firstResource = "";

	public String lastResource = "";

	public String resource;

	public BloomFilterI filter = null;

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

	public SuperBucket(BloomFilterI filter, String firstResource, String lastResource) {
		this.filter = filter;
		this.firstResource = firstResource;
		this.lastResource = lastResource;
	}

	// @SuppressWarnings("unchecked")
	public void run() {

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

				resourcesFileSorted = new File(LODVaderProperties.TMP_FOLDER + "sorted_tmp");

				File tmpFolder = new File(LODVaderProperties.TMP_FOLDER);

				Comparator<String> comparator = ExternalSort.defaultcomparator;
				List<File> l = ExternalSort.sortInBatch(resourcesFile, comparator, ExternalSort.DEFAULTMAXTEMPFILES,
						Charset.defaultCharset(), tmpFolder, true, 0, false);
				logger.info("created " + l.size() + " tmp files");
				ExternalSort.mergeSortedFiles(l, resourcesFileSorted, comparator, Charset.defaultCharset(), true, false,
						false);

				BufferedReader f = new BufferedReader(new FileReader(resourcesFileSorted));

				if (COLLECTION_NAME.equals(SubjectsBucket.SUBJECTS_BUCKET_COLLECTION_NAME)) {
					while ((resource = f.readLine()) != null) {
						chunk.add(resource);
						if (chunk.size() == chunkSize) {
							saveChunk((ArrayList<String>) chunk.clone());
							chunk = new ArrayList<String>();
						}
					}
				}

				// if we are saving objects, count how many of them are being
				// describes as subjects
				else {
					
					DistributionBloomFilterContainer subjects = new DistributionBloomFilterContainer(this.distributionID);
					int objectCohesion = 0;
					
					while ((resource = f.readLine()) != null) {
						chunk.add(resource);
						
						if(subjects.querySubject(resource)) 
							objectCohesion ++;
						
						if (chunk.size() == chunkSize) {
							saveChunk((ArrayList<String>) chunk.clone());
							chunk = new ArrayList<String>();
						}
					}
					
					DistributionDB distribution = new DistributionDB(this.distributionID);
					distribution.setObjectCohesion(objectCohesion);
					distribution.update(false);
					
				}
				f.close();
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
		GridFS gridFS = new GridFS(DBSuperClass2.getDBInstance(), COLLECTION_NAME);
		gridFS.remove(new BasicDBObject(DISTRIBUTION_ID, distributionID));
	}

	private void saveChunk(final ArrayList<String> chunk) {
		final int distributionID = this.distributionID;
		makeBloomFilter(chunk, distributionID);
	}

	private void makeBloomFilter(ArrayList<String> chunk, int distributionID) {

		final String firstResource = chunk.get(0);
		final String lastResource = chunk.get(chunk.size() - 1);
		int chunkSize = chunk.size();
		if (chunkSize < 5000)
			chunkSize = 5000;
		BloomFilterI filter = BloomFilterFactory.newBloomFilter();
		filter.create(chunkSize, fpp);
		for (String resource : chunk) {
			filter.add(resource);
		}

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		try {
			filter.writeTo(out);
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
		BloomFilterI filter = BloomFilterFactory.newBloomFilter();
		if (file != null)
			try {
				filter.readFrom(file.getInputStream());
				result = filter.compare(resource);

			} catch (IOException e) {
				e.printStackTrace();
			}
		// time=time+Double.parseDouble(t.stopTimer());
		// System.out.println(time);

		return result;
	}

	public BloomFilterI getFilter() {

		GridFS gfs = new GridFS(DBSuperClass2.getDBInstance(), COLLECTION_NAME);

		BasicDBObject firstResource = new BasicDBObject(FIRST_RESOURCE, new BasicDBObject("$lte", resource));
		BasicDBObject lastResource = new BasicDBObject(LAST_RESOURCE, new BasicDBObject("$gte", resource));
		BasicDBObject distribution = new BasicDBObject(DISTRIBUTION_ID, distributionID);

		BasicDBList and = new BasicDBList();
		and.add(firstResource);
		and.add(lastResource);
		and.add(distribution);

		// System.out.println(new BasicDBObject("$and",and));

		GridFSDBFile file = gfs.findOne(new BasicDBObject("$and", and));

		BloomFilterI filter = BloomFilterFactory.newBloomFilter();
		if (file != null)
			try {
				filter.readFrom(file.getInputStream());
				this.lastResource = file.get(LAST_RESOURCE).toString();
				this.firstResource = file.get(FIRST_RESOURCE).toString();
				this.filter = filter;
			} catch (IOException e) {
				e.printStackTrace();
			}

		return filter;
	}

	public ArrayList<SuperBucket> getFiltersFromDataset(int datasetID) {

		// get all distributions within the dataset
		ArrayList<Integer> distributionsIDs = new DatasetQueries().getDistributionsIDs(datasetID);

		ArrayList<SuperBucket> result = new ArrayList<SuperBucket>();

		// get collection
		GridFS gfs = new GridFS(DBSuperClass2.getDBInstance(), COLLECTION_NAME);

		// create query
		BasicDBObject in = new BasicDBObject("$in", distributionsIDs);

		BasicDBObject distributions = new BasicDBObject(DISTRIBUTION_ID, in);

		// make query
		List<GridFSDBFile> buckets = gfs.find(distributions);

		for (GridFSDBFile f : buckets) {
			BloomFilterI filter = BloomFilterFactory.newBloomFilter();
			try {
				filter.readFrom(f.getInputStream());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			result.add(new SuperBucket(filter, f.get(FIRST_RESOURCE).toString(), f.get(LAST_RESOURCE).toString()));
		}

		return result;
	}

}
