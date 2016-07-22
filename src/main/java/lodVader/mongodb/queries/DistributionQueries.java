package lodVader.mongodb.queries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.LODVaderProperties;
import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;
import lodVader.bloomfilters.models.LoadedBloomFiltersCache;
import lodVader.enumerators.TuplePart;
import lodVader.linksets.DistributionBloomFilterContainer;
import lodVader.mongodb.DBSuperClass2;
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
	public int distributionQuerySize;
	private NSUtils nsUtils = new NSUtils();

	AggregationOptions aggregationOptions = AggregationOptions.builder().batchSize(100)
			.outputMode(AggregationOptions.OutputMode.CURSOR).allowDiskUse(true).build();

	public HashSet<Integer> getDistributionsBySubjectNS(String nsToSearch) {

		BasicDBObject query = new BasicDBObject(DistributionSubjectNS0DB.NS, nsToSearch);
		DBCollection collection = DBSuperClass2.getDBInstance().getCollection(DistributionSubjectNS0DB.COLLECTION_NAME);

		HashSet<Integer> hash = new HashSet<Integer>();

		Cursor cursor = collection.find(query);

		while (cursor.hasNext()) {
			DBObject instance = cursor.next();
			hash.add(((Number) instance.get(DistributionSubjectNS0DB.DISTRIBUTION_ID)).intValue());
		}
		
		return hash; 
	}

	
	public HashSet<Integer> getDistributionsByObjectNS(String nsToSearch) {

		BasicDBObject query = new BasicDBObject(DistributionObjectNS0DB.NS, nsToSearch);
		DBCollection collection = DBSuperClass2.getDBInstance().getCollection(DistributionObjectNS0DB.COLLECTION_NAME);

		HashSet<Integer> hash = new HashSet<Integer>();

		Cursor cursor = collection.find(query);

		while (cursor.hasNext()) {
			DBObject instance = cursor.next();
			hash.add(((Number) instance.get(DistributionObjectNS0DB.DISTRIBUTION_ID)).intValue());
		}
		
		return hash; 
	}
	
	public ArrayList<DistributionDB> getDistributionsByOutdegree(ArrayList<String> nsToSearch,
			ConcurrentHashMap<Integer, DistributionBloomFilterContainer> distributionFilter) {
		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();

		try {

			// query all NS
			BasicDBObject query = new BasicDBObject(DistributionSubjectNS0DB.NS, new BasicDBObject("$in", nsToSearch));

			DBCollection collection = DBSuperClass2.getDBInstance()
					.getCollection(DistributionSubjectNS0DB.COLLECTION_NAME);

			// group by
			Cursor cursor = collection
					.aggregate(Arrays.asList(
							new BasicDBObject("$match",
									new BasicDBObject(DistributionSubjectNS0DB.NS,
											new BasicDBObject("$in", nsToSearch))),
							new BasicDBObject("$group",
									new BasicDBObject("_id", "$" + DistributionSubjectNS0DB.DISTRIBUTION_ID))

			), aggregationOptions);

			// save a list with distribution and fqdn
			while (cursor.hasNext()) {
				DBObject instance = cursor.next();

				DistributionDB distribution = new DistributionDB();
				distribution.setLodVaderID(((Number) instance.get("_id")).intValue());
				if (distribution.find(true)) {
					list.add(distribution);
					if (!distributionFilter.containsKey(distribution.getLODVaderID())) {
						distributionFilter.put(distribution.getLODVaderID(),
								new DistributionBloomFilterContainer(distribution.getLODVaderID()));
					}
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public BloomFilterI getDescribedNS0(String resourceType) {
		List<String> cursor;
		if (resourceType.equals(TuplePart.SUBJECT)) {
			DBCollection collection = DBSuperClass2.getDBInstance()
					.getCollection(DistributionSubjectNS0DB.COLLECTION_NAME);
			cursor = collection.distinct(DistributionSubjectNS0DB.NS);
		} else {
			DBCollection collection = DBSuperClass2.getDBInstance()
					.getCollection(DistributionObjectNS0DB.COLLECTION_NAME);
			cursor = collection.distinct(DistributionObjectNS0DB.NS);
		}
		int size = cursor.size();
		if (size < 5000)
			size = 5000;
		BloomFilterI g = BloomFilterFactory.newBloomFilter();

		g.create(cursor.size(), 0.00001);

		for (String s : cursor) {
			g.add(s);
		}
		return g;
	}

	public BloomFilterI getDescribedNS(TuplePart resourceType) {
		DBObject groupIdFields = null;

		if (resourceType.equals(TuplePart.OBJECT))
			groupIdFields = new BasicDBObject("_id", "$" + DistributionObjectNSDB.NS);
		else if (resourceType.equals(TuplePart.SUBJECT))
			groupIdFields = new BasicDBObject("_id", "$" + DistributionSubjectNSDB.NS);

		// groupIdFields.put("count", new BasicDBObject("$sum", 1));
		DBObject group = new BasicDBObject("$group", groupIdFields);

		DBObject projectFields = new BasicDBObject("_id", 0);

		if (resourceType.equals(TuplePart.OBJECT))
			projectFields.put(DistributionObjectNSDB.NS, "$_id");
		else if (resourceType.equals(TuplePart.SUBJECT))
			projectFields.put(DistributionSubjectNSDB.NS, "$_id");

		// projectFields.put("count", new BasicDBObject("$sum", 1));
		DBObject project = new BasicDBObject("$project", projectFields);

		ArrayList<DBObject> ag = new ArrayList<DBObject>();
		ag.add(group);
		ag.add(project);

		AggregationOptions options = AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.CURSOR)
				.allowDiskUse(true).build();

		BloomFilterI g = null;

		if (resourceType.equals(TuplePart.OBJECT)) {

			DBCollection collection = DBSuperClass2.getDBInstance()
					.getCollection(DistributionObjectNSDB.COLLECTION_NAME);

			g = BloomFilterFactory.newBloomFilter();
			g.create(collection.find().size() + LODVaderProperties.BF_BUFFER_RANGE, 0.0001);

			int size = 0;
			Cursor aggregate = collection.aggregate(ag, options);
			while (aggregate.hasNext()) {
				DBObject d = aggregate.next();
				g.add(d.get(DistributionObjectNSDB.NS).toString());
				size++;
			}

			logger.info("Loaded " + size + " object namespaces.");

		} else if (resourceType.equals(TuplePart.SUBJECT)) {

			DBCollection collection = DBSuperClass2.getDBInstance()
					.getCollection(DistributionSubjectNSDB.COLLECTION_NAME);

			g = BloomFilterFactory.newBloomFilter();
			g.create(collection.find().size() + LODVaderProperties.BF_BUFFER_RANGE, 0.0001);

			Cursor aggregate = collection.aggregate(ag, options);
			int size = 0;
			while (aggregate.hasNext()) {
				DBObject d = aggregate.next();
				g.add(d.get(DistributionSubjectNSDB.NS).toString());
				size++;
			}
			logger.info("Loaded " + size + " subject namespaces.");

		}

		return g;
	}

	public ArrayList<DistributionDB> getDistributionsByIndegree(ArrayList<String> fqdnToSearch,
			ConcurrentHashMap<Integer, DistributionBloomFilterContainer> fqdnPerDistribution) {
		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();
		HashSet<Integer> map = new HashSet<Integer>();
		try {

			BasicDBObject query = new BasicDBObject(DistributionObjectNS0DB.NS, new BasicDBObject("$in", fqdnToSearch));

			DBCollection collection = DBSuperClass2.getDBInstance()
					.getCollection(DistributionObjectNS0DB.COLLECTION_NAME);

			// fileds to be projected
			BasicDBObject project = new BasicDBObject(DistributionSubjectNS0DB.DISTRIBUTION_ID, 1);

			DBCursor cursor = collection.find(query, project);

			while (cursor.hasNext()) {
				DBObject instance = cursor.next();
				if (!map.contains(((Integer) instance.get(DistributionSubjectNS0DB.DISTRIBUTION_ID)).intValue())) {

					// DistributionDB distribution = new DistributionDB(
					// ((Number)
					// instance.get(DistributionObjectNS0DB.DISTRIBUTION_ID)).intValue());

					DistributionDB distribution = new DistributionDB();
					distribution
							.setLodVaderID(((Number) instance.get(DistributionObjectNS0DB.DISTRIBUTION_ID)).intValue());
					if (distribution.find(true)) {
						list.add(distribution);

						if (!fqdnPerDistribution.containsKey(distribution.getUri())) {
							fqdnPerDistribution.put(distribution.getLODVaderID(),
									new DistributionBloomFilterContainer(distribution.getLODVaderID()));
						}
					}
					map.add(((Integer) instance.get(DistributionSubjectNS0DB.DISTRIBUTION_ID)).intValue());

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
	public Double getNumberOfTriples() {
		Double numberOfTriples = 0.0;
		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(DistributionDB.COLLECTION_NAME);

			BasicDBObject select = new BasicDBObject("$match",
					new BasicDBObject(DistributionDB.SUCCESSFULLY_DOWNLOADED, true));

			BasicDBObject groupFields = new BasicDBObject("_id", null);

			groupFields.append("sum", new BasicDBObject("$sum", "$triples"));

			DBObject group = new BasicDBObject("$group", groupFields);

			// run aggregation
			List<DBObject> pipeline = Arrays.asList(select, group);
			AggregationOutput output = collection.aggregate(pipeline);

			for (DBObject result : output.results()) {
				numberOfTriples = Double.valueOf(result.get("sum").toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return numberOfTriples;
	}

	/**
	 * 
	 * @return number of total triples by vocab
	 */
	public long getNumberOfTriples(Boolean isVocab) {
		long totalTriples = 0;

		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(DistributionDB.COLLECTION_NAME);

			BasicDBObject query;

			if (isVocab != null)
				query = new BasicDBObject(new BasicDBObject(DistributionDB.IS_VOCABULARY, isVocab));
			else
				query = new BasicDBObject();

			DBCursor instances = collection.find(query);

			Iterator<DBObject> it = instances.iterator();

			while (it.hasNext()) {
				totalTriples = totalTriples + Long.parseLong(it.next().get(DistributionDB.TRIPLES).toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return totalTriples;
	}

	/**
	 * Get all distributions
	 * 
	 * @param vocabularies
	 *            specifies whether should vocabularies be added in the return
	 *            list. If the value is null, vocabularies ans distrubitions
	 *            will be returned
	 * @return a ArrayList of DistributionMongoDBObject
	 */
	public ArrayList<DistributionDB> getDistributions(Boolean vocabularies, String status, Integer datasetID) {

		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();

		DBCursor instances;

		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(DistributionDB.COLLECTION_NAME);

			BasicDBList and = new BasicDBList();

			if (vocabularies != null) {
				if (vocabularies)
					and.add(new BasicDBObject(DistributionDB.IS_VOCABULARY, true));
				else
					and.add(new BasicDBObject(DistributionDB.IS_VOCABULARY, false));
			}

			if (status != null && status != "")
				and.add(new BasicDBObject(DistributionDB.STATUS, status));

			if (datasetID != null)
				and.add(new BasicDBObject(DistributionDB.TOP_DATASET, datasetID));

			if (and.size() > 0)
				instances = collection.find(new BasicDBObject("$and", and));
			else
				instances = collection.find();

			for (DBObject instance : instances) {
				list.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * Get distributions using filters
	 * 
	 * @param skip
	 *            how many distribution to skip
	 * @param limit
	 *            size of the range
	 * @param searchVocabularies
	 *            true only for vocabularies, false only for datasets and null
	 *            for vocabularies and datasets
	 * @param seach
	 *            string to compare with distribution name or downloadurl
	 * @param searchStatus
	 *            search status: DONE, ERROR, WAITING_TO_STREAM or STREAMING.
	 * @return a ArrayList of DistributionMongoDBObject
	 */
	public ArrayList<DistributionDB> getDistributions(int skip, int limit, Boolean searchVocabularies,
			String searchNameOrURL, List<Integer> in, String searchStatus) {

		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();

		try {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(DistributionDB.COLLECTION_NAME);

			DBObject query = null;

			if (searchNameOrURL != "") {
				DBObject query2;
				DBObject query3;
				query2 = new BasicDBObject(DistributionDB.DOWNLOAD_URL,
						java.util.regex.Pattern.compile(searchNameOrURL));
				query3 = new BasicDBObject(DistributionDB.TITLE, java.util.regex.Pattern.compile(searchNameOrURL));

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

			if (searchVocabularies != null) {
				BasicDBList and = new BasicDBList();
				if (query != null)
					and.add(query);
				and.add(new BasicDBObject(DistributionDB.IS_VOCABULARY, searchVocabularies));
				query = new BasicDBObject("$and", and);
			}

			if (!searchStatus.equals("")) {
				BasicDBList and = new BasicDBList();
				if (query != null)
					and.add(query);
				and.add(new BasicDBObject(DistributionDB.STATUS, searchStatus));
				query = new BasicDBObject("$and", and);
			}

			logger.debug("MongoDB query: " + query);
			DBCursor instances = collection.find(query);
			distributionQuerySize = instances.size();

			BasicDBObject sort = new BasicDBObject(DistributionDB.TRIPLES, -1);
			instances = collection.find(query).skip(skip).limit(limit).sort(sort);

			for (DBObject instance : instances) {
				list.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// return all distributions
	public ArrayList<DistributionDB> getDistributionsByTopDatasetURL(DatasetDB topDataset) {

		ArrayList<DistributionDB> distributionList = new ArrayList<DistributionDB>();

		DBCollection collection;

		try {
			collection = DBSuperClass2.getDBInstance().getCollection(DistributionDB.COLLECTION_NAME);
			DBCursor instances = collection
					.find(new BasicDBObject(DistributionDB.DEFAULT_DATASETS, topDataset.getLODVaderID()));

			for (DBObject instance : instances) {
				distributionList.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return distributionList;
	}

	// return all distributions
	public ArrayList<DistributionDB> getDistributionsByCohesion() {

		ArrayList<DistributionDB> distributionList = new ArrayList<DistributionDB>();

		DBCollection collection;

		try {
			collection = DBSuperClass2.getDBInstance().getCollection(DistributionDB.COLLECTION_NAME);
			DBCursor instances = collection.find().sort(new BasicDBObject(DistributionDB.OBJECTS_COHESION, 1));

			for (DBObject instance : instances) {
				distributionList.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return distributionList;
	}

	// return all distributions
	public ArrayList<DistributionDB> getSetOfDistributions(Set<Integer> set) {

		ArrayList<DistributionDB> distributionList = new ArrayList<DistributionDB>();

		DBCollection collection = DBSuperClass2.getDBInstance().getCollection(DatasetDB.COLLECTION_NAME);

		try {
			collection = DBSuperClass2.getDBInstance().getCollection(DistributionDB.COLLECTION_NAME);
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

	// @Test
	// public void queryDistribution(){
	public HashSet<DistributionDB> queryDistribution(String resource, TuplePart type) {
		// String resource = "http://www.w3.org/2005/11/its/rdf#taSource";
		// String type = LODVaderProperties.TYPE_SUBJECT;
		HashSet<DistributionDB> setOfDistributionNS = new HashSet<DistributionDB>();

		// get resource fqdn

		String ns = nsUtils.getNSFromString(resource);

		if (type.equals(TuplePart.SUBJECT)) {
			DBCollection collection = DBSuperClass2.getCollection(DistributionSubjectNSDB.COLLECTION_NAME);

			DBObject query;
			query = new BasicDBObject(DistributionSubjectNSDB.NS, ns);

			DBCursor instances = collection.find(query);

			ArrayList<LoadedBloomFiltersCache> cache = new ArrayList<LoadedBloomFiltersCache>();

			for (DBObject instance : instances) {
				DistributionDB d = new DistributionDB(
						Integer.valueOf(instance.get(DistributionSubjectNSDB.DISTRIBUTION_ID).toString()));
				LoadedBloomFiltersCache l = new LoadedBloomFiltersCache(d, resource, TuplePart.SUBJECT);
				l.start();
				cache.add(l);
			}
			for (LoadedBloomFiltersCache l : cache) {
				try {
					l.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			for (LoadedBloomFiltersCache l : cache) {

				if (l.found) {
					setOfDistributionNS.add(l.getDistribution());
				}
			}
		}

		else if (type.equals(TuplePart.OBJECT)) {
			DBCollection collection = DBSuperClass2.getDBInstance()
					.getCollection(DistributionObjectNSDB.COLLECTION_NAME);

			DBObject query;
			query = new BasicDBObject(DistributionObjectNSDB.NS, ns);

			DBCursor instances = collection.find(query);

			ArrayList<LoadedBloomFiltersCache> cache = new ArrayList<LoadedBloomFiltersCache>();

			for (DBObject instance : instances) {
				DistributionDB d = new DistributionDB(
						Integer.valueOf(instance.get(DistributionSubjectNSDB.DISTRIBUTION_ID).toString()));
				LoadedBloomFiltersCache l = new LoadedBloomFiltersCache(d, resource, TuplePart.OBJECT);
				l.start();
				cache.add(l);
			}
			for (LoadedBloomFiltersCache l : cache) {
				try {
					l.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			for (LoadedBloomFiltersCache l : cache) {
				if (l.found)
					setOfDistributionNS.add(l.getDistribution());

			}
		}

		else if (type.equals(TuplePart.PROPERTY)) {
			DBCollection collection = DBSuperClass2.getDBInstance().getCollection(AllPredicatesDB.COLLECTION_NAME);

			DBObject query;
			query = new BasicDBObject(AllPredicatesDB.URI, resource);

			DBCursor instances = collection.find(query);

			int predicateID;

			if (instances.iterator().hasNext())
				predicateID = Integer
						.parseInt(instances.iterator().next().get(AllPredicatesDB.LOD_VADER_ID).toString());
			else
				return setOfDistributionNS;

			collection = DBSuperClass2.getDBInstance().getCollection(AllPredicatesRelationDB.COLLECTION_NAME);
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
