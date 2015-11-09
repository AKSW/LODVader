package lodVader.mongodb.queries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.LODVaderProperties;
import lodVader.LoadedBloomFiltersCache;
import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.linksets.DistributionFilter;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import lodVader.mongodb.collections.namespaces.DistributionObjectNS0DB;
import lodVader.mongodb.collections.namespaces.DistributionObjectNSDB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNS0DB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;
import lodVader.utils.NSUtils;

public class DistributionQueries {

	final static Logger logger = LoggerFactory.getLogger(DistributionQueries.class);

	public int getDistributionQuerySize;

	NSUtils nsUtils = new NSUtils();

	public ArrayList<DistributionDB> getDistributionsByOutdegree(ArrayList<String> nsToSearch,
			ConcurrentHashMap<Integer, DistributionFilter> distributionFilter) {
		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();
		try {

			// query all NS
			BasicDBObject query = new BasicDBObject(DistributionSubjectNS0DB.SUBJECT_NS0,
					new BasicDBObject("$in", nsToSearch));

			DBCollection collection = DBSuperClass.getInstance()
					.getCollection(DistributionSubjectNS0DB.COLLECTION_NAME);

			DBCursor cursor = collection.find(query);

			// save a list with distribution and fqdn
			while (cursor.hasNext()) {
				DBObject instance = cursor.next();
				DistributionDB distribution = new DistributionDB(
						((Number) instance.get(DistributionSubjectNS0DB.DISTRIBUTION_ID)).intValue());
				list.add(distribution);

				if (!distributionFilter.containsKey(distribution.getLODVaderID())) {
					distributionFilter.put(distribution.getLODVaderID(),
							new DistributionFilter(distribution.getLODVaderID()));

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public GoogleBloomFilter getDescribedNS0(String resourceType) {
		List<String> cursor;
		if (resourceType.equals(LODVaderProperties.TYPE_SUBJECT)) {
			DBCollection collection = DBSuperClass.getInstance()
					.getCollection(DistributionSubjectNS0DB.COLLECTION_NAME);
			cursor = collection.distinct(DistributionSubjectNS0DB.SUBJECT_NS0);
		} else {
			DBCollection collection = DBSuperClass.getInstance().getCollection(DistributionObjectNS0DB.COLLECTION_NAME);
			cursor = collection.distinct(DistributionObjectNS0DB.OBJECT_NS0);
		}
		int size = cursor.size();
		if (size < 5000)
			size = 5000;
		GoogleBloomFilter g = new GoogleBloomFilter(cursor.size(), 0.00001);

		for (String s : cursor) {
			g.add(s);
		}
		return g;
	}

//	@Test
//	public void getDescribedNS() {
		 public GoogleBloomFilter getDescribedNS(String resourceType){
		// if(resourceType.equals(LODVaderProperties.TYPE_SUBJECT)){
		// DBCollection collection = DBSuperClass.getInstance()
		// .getCollection(DistributionSubjectNSDB.COLLECTION_NAME);
		// cursor = collection.distinct(DistributionSubjectNSDB.SUBJECT_NS);
		// }
		// else{
//		String resourceType = LODVaderProperties.TYPE_OBJECT;

		DBObject groupIdFields = null;

		if (resourceType.equals(LODVaderProperties.TYPE_OBJECT))
			groupIdFields = new BasicDBObject("_id", "$" + DistributionObjectNSDB.OBJECT_NS);
		else if (resourceType.equals(LODVaderProperties.TYPE_SUBJECT))
			groupIdFields = new BasicDBObject("_id", "$" + DistributionSubjectNSDB.SUBJECT_NS);

		// groupIdFields.put("count", new BasicDBObject("$sum", 1));
		DBObject group = new BasicDBObject("$group", groupIdFields);

		DBObject projectFields = new BasicDBObject("_id", 0);

		if (resourceType.equals(LODVaderProperties.TYPE_OBJECT))
			projectFields.put(DistributionObjectNSDB.OBJECT_NS, "$_id");
		else if (resourceType.equals(LODVaderProperties.TYPE_SUBJECT))
			projectFields.put(DistributionSubjectNSDB.SUBJECT_NS, "$_id");

		// projectFields.put("count", new BasicDBObject("$sum", 1));
		DBObject project = new BasicDBObject("$project", projectFields);

		ArrayList<DBObject> ag = new ArrayList<DBObject>();
		ag.add(group);
		ag.add(project);

		AggregationOptions options = AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.CURSOR)
				.allowDiskUse(true).build();

		GoogleBloomFilter g = null;

		if (resourceType.equals(LODVaderProperties.TYPE_OBJECT)) {

			DBCollection collection = DBSuperClass.getInstance().getCollection(DistributionObjectNSDB.COLLECTION_NAME);

			g = new GoogleBloomFilter(collection.find().size(), 0.0001);

			Cursor aggregate = collection.aggregate(ag, options);
			while (aggregate.hasNext()) {
				DBObject d = aggregate.next();
				g.add(d.get(DistributionObjectNSDB.OBJECT_NS).toString());
			}
		}
		else if (resourceType.equals(LODVaderProperties.TYPE_SUBJECT)) {

			DBCollection collection = DBSuperClass.getInstance().getCollection(DistributionSubjectNSDB.COLLECTION_NAME);

			g = new GoogleBloomFilter(collection.find().size(), 0.0001);

			Cursor aggregate = collection.aggregate(ag, options);
			while (aggregate.hasNext()) {
				DBObject d = aggregate.next();
				g.add(d.get(DistributionSubjectNSDB.SUBJECT_NS).toString());
			}
		}
		

//		if (resourceType.equals(LODVaderProperties.TYPE_SUBJECT))
//			while (aggregate.hasNext()) {
//				DBObject d = aggregate.next();
//				System.out.println(d.get(DistributionSubjectNSDB.SUBJECT_NS));
//			}

		// for(DBObject b:aggregate.g){
		// System.out.println(b.get(DistributionObjectNSDB.OBJECT_NS));
		// }

		// }
		// int size = cursor.size();
		// if(size<5000) size = 5000;
		//
		// for(String s: cursor){
		// g.add(s);
		// }
		 return g;
	}

	public ArrayList<DistributionDB> getDistributionsByIndegree(ArrayList<String> fqdnToSearch,
			ConcurrentHashMap<Integer, DistributionFilter> fqdnPerDistribution) {
		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();
		try {

			// find distributions with subjects
			// BasicDBObject query = new
			// BasicDBObject(DistributionObjectNSDB.OBJECT_NS,
			// new BasicDBObject("$in", fqdnToSearch));
			BasicDBObject query = new BasicDBObject(DistributionObjectNS0DB.OBJECT_NS0,
					new BasicDBObject("$in", fqdnToSearch));

			DBCollection collection = DBSuperClass.getInstance().getCollection(DistributionObjectNS0DB.COLLECTION_NAME);

			DBCursor cursor = collection.find(query);

			while (cursor.hasNext()) {
				DBObject instance = cursor.next();
				DistributionDB distribution = new DistributionDB(
						((Number) instance.get(DistributionObjectNS0DB.DISTRIBUTION_ID)).intValue());

				list.add(distribution);

				if (!fqdnPerDistribution.containsKey(distribution.getUri())) {
					fqdnPerDistribution.put(distribution.getLODVaderID(),
							new DistributionFilter(distribution.getLODVaderID()));
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * 
	 * @return number of total triples read
	 */
	public int getNumberOfTriples() {
		int numberOfTriples = 0;
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(DistributionDB.COLLECTION_NAME);

			BasicDBObject select = new BasicDBObject("$match",
					new BasicDBObject(DistributionDB.SUCCESSFULLY_DOWNLOADED, true));

			BasicDBObject groupFields = new BasicDBObject("_id", null);

			groupFields.append("sum", new BasicDBObject("$sum", "$triples"));

			DBObject group = new BasicDBObject("$group", groupFields);

			// run aggregation
			List<DBObject> pipeline = Arrays.asList(select, group);
			AggregationOutput output = collection.aggregate(pipeline);

			for (DBObject result : output.results()) {
				numberOfTriples = Integer.valueOf(result.get("sum").toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return numberOfTriples;
	}

	/**
	 * Get distributions
	 * 
	 * @param withVocabularies
	 *            specifies whether should vocabularies are added to the return
	 *            list
	 * @return a ArrayList of DistributionMongoDBObject
	 */
	public ArrayList<DistributionDB> getDistributions(boolean withVocabularies) {

		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(DistributionDB.COLLECTION_NAME);
			DBCursor instances = collection.find();

			for (DBObject instance : instances) {
				list.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * Get distributions in a range
	 * 
	 * @param skip
	 *            initial value of range
	 * @param limit
	 *            final value of range
	 * @return a ArrayList of DistributionMongoDBObject
	 */
	public ArrayList<DistributionDB> getDistributions(int skip, int limit, int searchVocabularies,
			String downloadURLSearch, List<Integer> in, int searchStatus) {

		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(DistributionDB.COLLECTION_NAME);

			DBObject query = null;
			// query = new BasicDBObject(DistributionDB.IS_VOCABULARY, true);

			if (downloadURLSearch != null) {
				DBObject query2;
				DBObject query3;
				// System.out.println(search);
				// System.out.println(isVocabulary);
				// query3 = new BasicDBObject(DistributionDB.IS_VOCABULARY,
				// isVocabulary);
				// query2 = new
				// BasicDBObject(DistributionMongoDBObject.DOWNLOAD_URL, new
				// BasicDBObject("$regex",""+search+""));
				query2 = new BasicDBObject(DistributionDB.DOWNLOAD_URL,
						java.util.regex.Pattern.compile(downloadURLSearch));
				query3 = new BasicDBObject(DistributionDB.TITLE, java.util.regex.Pattern.compile(downloadURLSearch));

				// DatasetMongoDBObject.URI, /.*m.*/
				// new BasicDBObject("$regex", topDataset + ".*")

				// make a AND operator
				BasicDBList or = new BasicDBList();
				or.add(query3);
				or.add(query2);
				query = new BasicDBObject("$or", or);
			}

			if (in.size() > 0) {
				BasicDBList and = new BasicDBList();
				if (query != null)
					and.add(query);
				and.add(new BasicDBObject(DistributionDB.LOD_VADER_ID, new BasicDBObject("$in", in)));
				query = new BasicDBObject("$and", and);
			}

			if (searchVocabularies == 0) {
				BasicDBList and = new BasicDBList();
				if (query != null)
					and.add(query);
				and.add(new BasicDBObject(DistributionDB.IS_VOCABULARY, true));
				query = new BasicDBObject("$and", and);
			}
			if (searchVocabularies == 1) {
				BasicDBList and = new BasicDBList();
				if (query != null)
					and.add(query);
				and.add(new BasicDBObject(DistributionDB.IS_VOCABULARY, false));
				query = new BasicDBObject("$and", and);
			}
			
			if (searchStatus == 0) {
				BasicDBList and = new BasicDBList();
				if (query != null)
					and.add(query);
				and.add(new BasicDBObject(DistributionDB.STATUS, DistributionDB.STATUS_DONE));
				query = new BasicDBObject("$and", and);
			}
			else if (searchStatus == 1) {
				BasicDBList and = new BasicDBList();
				if (query != null)
					and.add(query);
				and.add(new BasicDBObject(DistributionDB.STATUS, DistributionDB.STATUS_WAITING_TO_STREAM));
				query = new BasicDBObject("$and", and);
			}
			else if (searchStatus == 2) {
				BasicDBList and = new BasicDBList();
				if (query != null)
					and.add(query);
				and.add(new BasicDBObject(DistributionDB.STATUS, DistributionDB.STATUS_ERROR));
				query = new BasicDBObject("$and", and);
			}

			// DBCursor inst = collection.find(new BasicDBObject("$and", and));

			DBCursor instances = collection.find(query);
			getDistributionQuerySize = instances.size();
			instances = collection.find(query).skip(skip).limit(limit);

			for (DBObject instance : instances) {
				list.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// return all distributions
	public ArrayList<DistributionDB> getDistributionsByTopDatasetID(String topDataset) {

		ArrayList<DistributionDB> distributionList = new ArrayList<DistributionDB>();

		ArrayList<Integer> datasetList = new ArrayList<Integer>();

		DBCollection collection = DBSuperClass.getInstance().getCollection(DatasetDB.COLLECTION_NAME);

		// find address by URI...
		BasicDBObject uriQuery = new BasicDBObject(DatasetDB.URI, new BasicDBObject("$regex", topDataset + ".*"));

		// ... or by access url
		BasicDBObject accessQuery = new BasicDBObject(DatasetDB.ACCESS_URL,
				new BasicDBObject("$regex", topDataset + ".*"));

		// make a OR operator
		BasicDBList or = new BasicDBList();
		or.add(uriQuery);
		or.add(accessQuery);

		DBCursor inst = collection.find(new BasicDBObject("$or", or));

		while (inst.hasNext()) {
			datasetList.add(((Number) inst.next().get(DatasetDB.LOD_VADER_ID)).intValue());
		}

		try {
			collection = DBSuperClass.getInstance().getCollection(DistributionDB.COLLECTION_NAME);
			DBCursor instances = collection
					.find(new BasicDBObject(DistributionDB.DEFAULT_DATASETS, new BasicDBObject("$in", datasetList)));

			for (DBObject instance : instances) {
				distributionList.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return distributionList;
	}

	// return all distributions
	public ArrayList<DistributionDB> getSetOfDistributions(HashSet<Integer> set) {

		ArrayList<DistributionDB> distributionList = new ArrayList<DistributionDB>();

		DBCollection collection = DBSuperClass.getInstance().getCollection(DatasetDB.COLLECTION_NAME);

		try {
			collection = DBSuperClass.getInstance().getCollection(DistributionDB.COLLECTION_NAME);
			DBCursor instances = collection
					.find(new BasicDBObject(DistributionDB.LOD_VADER_ID, new BasicDBObject("$in", set)));

			for (DBObject instance : instances) {
				distributionList.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return distributionList;
	}

	public HashSet<DistributionDB> getDistributionsByResource(String resource, String type) {

		HashSet<DistributionDB> setOfDistributionNS = new HashSet<DistributionDB>();

		// get resource fqdn

		String ns = nsUtils.getNSFromString(resource);

		if (type.equals(LODVaderProperties.TYPE_SUBJECT)) {
			DBCollection collection = DBSuperClass.getInstance().getCollection(DistributionSubjectNSDB.COLLECTION_NAME);

			DBObject query;
			query = new BasicDBObject(DistributionSubjectNSDB.SUBJECT_NS, ns);

			DBCursor instances = collection.find(query);
			
			ArrayList<LoadedBloomFiltersCache> cache = new ArrayList<LoadedBloomFiltersCache>();

			for (DBObject instance : instances) {
				DistributionDB d = new DistributionDB(
						Integer.valueOf(instance.get(DistributionSubjectNSDB.DISTRIBUTION_ID).toString()));
				LoadedBloomFiltersCache l = new LoadedBloomFiltersCache(d, resource, LODVaderProperties.TYPE_SUBJECT);
				l.start();
				cache.add(l);
			}
			for(LoadedBloomFiltersCache l : cache){
				try {
					l.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			for(LoadedBloomFiltersCache l : cache)
			{
				if (l.found)
					setOfDistributionNS.add(l.distribution);
				
			}
		}

		else if (type.equals(LODVaderProperties.TYPE_OBJECT)) {
			DBCollection collection = DBSuperClass.getInstance().getCollection(DistributionObjectNSDB.COLLECTION_NAME);

			DBObject query;
			query = new BasicDBObject(DistributionObjectNSDB.OBJECT_NS, ns);

			DBCursor instances = collection.find(query);

			ArrayList<LoadedBloomFiltersCache> cache = new ArrayList<LoadedBloomFiltersCache>();

			for (DBObject instance : instances) {
				DistributionDB d = new DistributionDB(
						Integer.valueOf(instance.get(DistributionSubjectNSDB.DISTRIBUTION_ID).toString()));
				LoadedBloomFiltersCache l = new LoadedBloomFiltersCache(d, resource, LODVaderProperties.TYPE_OBJECT);
				l.start();
				cache.add(l);
			}
			for(LoadedBloomFiltersCache l : cache){
				try {
					l.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			for(LoadedBloomFiltersCache l : cache)
			{
				if (l.found)
					setOfDistributionNS.add(l.distribution);
				
			}
		}

		else if (type.equals(LODVaderProperties.TYPE_PROPERTY)) {
			DBCollection collection = DBSuperClass.getInstance().getCollection(AllPredicatesDB.COLLECTION_NAME);

			DBObject query;
			query = new BasicDBObject(AllPredicatesDB.URI, resource);

			DBCursor instances = collection.find(query);

			int predicateID;

			if (instances.iterator().hasNext())
				predicateID = Integer
						.parseInt(instances.iterator().next().get(new AllPredicatesDB().LOD_VADER_ID).toString());
			else
				return setOfDistributionNS;

			collection = DBSuperClass.getInstance().getCollection(AllPredicatesRelationDB.COLLECTION_NAME);
			query = new BasicDBObject(AllPredicatesRelationDB.PREDICATE_ID, predicateID);

			instances = collection.find(query);

			for (DBObject instance : instances) {
				DistributionDB d = new DistributionDB(
						Integer.valueOf(instance.get(AllPredicatesRelationDB.DISTRIBUTION_ID).toString()));
				setOfDistributionNS.add(d);
			}
		}

		return setOfDistributionNS;

	}

}
