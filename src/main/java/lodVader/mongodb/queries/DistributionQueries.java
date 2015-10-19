package lodVader.mongodb.queries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.LODVaderProperties;
import lodVader.LoadedBloomFiltersCache;
import lodVader.linksets.DistributionNS;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.DistributionObjectNSDB;
import lodVader.mongodb.collections.DistributionSubjectNSDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import lodVader.utils.NSUtils;

public class DistributionQueries {

	final static Logger logger = Logger.getLogger(DistributionQueries.class);
	
	public int getDistributionQuerySize;
	
	NSUtils nsUtils = new NSUtils();

	public ArrayList<DistributionDB> getDistributionsByOutdegree(ArrayList<String> fqdnToSearch,
			ConcurrentHashMap<Integer, DistributionNS> fqdnPerDistribution) {
		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();
		try {

			// query all fqdn
			BasicDBObject query = new BasicDBObject(DistributionSubjectNSDB.SUBJECT_NS,
					new BasicDBObject("$in", fqdnToSearch));

			DBCollection collection = DBSuperClass.getInstance()
					.getCollection(DistributionSubjectNSDB.COLLECTION_NAME);

			DBCursor cursor = collection.find(query);

			// save a list with distribution and fqdn
			while (cursor.hasNext()) {
				DBObject instance = cursor.next();
				DistributionDB distribution = new DistributionDB(
						((Number) instance.get(DistributionSubjectNSDB.DISTRIBUTION_ID)).intValue());
				list.add(distribution);

				if (!fqdnPerDistribution.containsKey(distribution.getLODVaderID())) {
					fqdnPerDistribution.put(distribution.getLODVaderID(),
							createDistributionFQDNObject(distribution.getLODVaderID()));

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public DistributionNS createDistributionFQDNObject(int distribution) {

		DistributionNS distributionFQDNObject = new DistributionNS();

		// query all objects fqdn for the distribution
		BasicDBObject subjectQuery = new BasicDBObject(DistributionSubjectNSDB.DISTRIBUTION_ID, distribution);

		DBCollection collection = DBSuperClass.getInstance()
				.getCollection(DistributionSubjectNSDB.COLLECTION_NAME);

		DBCursor cursor = collection.find(subjectQuery);

		TreeSet<String> subjectsFQDN = new TreeSet<String>();

		while (cursor.hasNext()) {
			subjectsFQDN.add(cursor.next().get(DistributionSubjectNSDB.SUBJECT_NS).toString());
		}

		// doing the same for objects fqdn
		BasicDBObject objectQuery = new BasicDBObject(DistributionObjectNSDB.DISTRIBUTION_ID, distribution);

		collection = DBSuperClass.getInstance().getCollection(DistributionObjectNSDB.COLLECTION_NAME);

		cursor = collection.find(objectQuery);

		TreeSet<String> objectsFQDN = new TreeSet<String>();

		while (cursor.hasNext()) {
			objectsFQDN.add(cursor.next().get(DistributionObjectNSDB.OBJECT_NS).toString());
		}

		distributionFQDNObject.addObjectsFQDN(objectsFQDN);
		distributionFQDNObject.addSubjectsFQDN(subjectsFQDN);
		distributionFQDNObject.distributionMongoDBObject = new DistributionDB(distribution);
		distributionFQDNObject.distribution = distributionFQDNObject.distributionMongoDBObject.getLODVaderID();

		return distributionFQDNObject;

	}

	public ArrayList<DistributionDB> getDistributionsByIndegree(ArrayList<String> fqdnToSearch,
			ConcurrentHashMap<Integer, DistributionNS> fqdnPerDistribution) {
		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();
		try {

			// find distributions with subjects
			BasicDBObject query = new BasicDBObject(DistributionObjectNSDB.OBJECT_NS,
					new BasicDBObject("$in", fqdnToSearch));

			DBCollection collection = DBSuperClass.getInstance()
					.getCollection(DistributionObjectNSDB.COLLECTION_NAME);

			DBCursor cursor = collection.find(query);

			while (cursor.hasNext()) {
				DBObject instance = cursor.next();
				DistributionDB distribution = new DistributionDB(
						((Number) instance.get(DistributionObjectNSDB.DISTRIBUTION_ID)).intValue());

				list.add(distribution);

				if (!fqdnPerDistribution.containsKey(distribution.getUri())) {
					fqdnPerDistribution.put(distribution.getLODVaderID(),
							createDistributionFQDNObject(distribution.getLODVaderID()));
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
	public ArrayList<DistributionDB> getDistributions(int skip, int limit, int searchVocabularies, String downloadURLSearch, List<Integer> in) {

		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(DistributionDB.COLLECTION_NAME);

			DBObject query = null;
//			query = new BasicDBObject(DistributionDB.IS_VOCABULARY, true);

			if (downloadURLSearch != null) {
				DBObject query2;
				DBObject query3;
				// System.out.println(search);
				// System.out.println(isVocabulary);
//				query3 = new BasicDBObject(DistributionDB.IS_VOCABULARY, isVocabulary);
				// query2 = new
				// BasicDBObject(DistributionMongoDBObject.DOWNLOAD_URL, new
				// BasicDBObject("$regex",""+search+""));
				query2 = new BasicDBObject(DistributionDB.DOWNLOAD_URL, java.util.regex.Pattern.compile(downloadURLSearch));
				query3 = new BasicDBObject(DistributionDB.TITLE, java.util.regex.Pattern.compile(downloadURLSearch));

				// DatasetMongoDBObject.URI, /.*m.*/
				// new BasicDBObject("$regex", topDataset + ".*")

				// make a AND operator
				BasicDBList or = new BasicDBList();
				or.add(query3);
				or.add(query2);
				query = new BasicDBObject("$or", or);
			}
			
			if(in.size()>0){
				BasicDBList and = new BasicDBList();
				if(query!=null)
					and.add(query);
				and.add(new BasicDBObject(DistributionDB.LOD_VADER_ID, new BasicDBObject("$in", in)));
				query = new BasicDBObject("$and", and);
			}
			
			if(searchVocabularies==0){
				BasicDBList and = new BasicDBList();
				if(query!=null)
					and.add(query);
				and.add(new BasicDBObject(DistributionDB.IS_VOCABULARY, true));
				query = new BasicDBObject("$and", and);
			}
			if(searchVocabularies==1){
				BasicDBList and = new BasicDBList();
				if(query!=null)
					and.add(query);
				and.add(new BasicDBObject(DistributionDB.IS_VOCABULARY, false));
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

	/**
	 * Count distributions
	 * 
	 * @param withVocabularies
	 *            parameter that set whether vocabularies should be included in
	 *            the result
	 * @return number of distributions
	 */
	public int countDistributions(boolean withVocabularies) {

		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();

		DBCursor instances;

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(DistributionDB.COLLECTION_NAME);

			DBObject query;

			query = new BasicDBObject(DistributionDB.IS_VOCABULARY, withVocabularies);

			instances = collection.find(query);

			return instances.count();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
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
	
	@Test
	public void oi(){
		System.out.println(getDistributionsByResource("http://aksw.org/N3/Reuters-128/16#char=156,172", 
				LODVaderProperties.TYPE_SUBJECT).size());
		System.out.println(getDistributionsByResource("http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#RFC5147String", 
				LODVaderProperties.TYPE_OBJECT).size());
		System.out.println(getDistributionsByResource("http://www.w3.org/2005/11/its/rdf#taSource", 
				LODVaderProperties.TYPE_PROPERTY).size());
		
	}

	public HashSet<DistributionDB> getDistributionsByResource(String resource, String type) {

		HashSet<DistributionDB> setOfDistributionFQDN = new HashSet<DistributionDB>();

		// get resource fqdn
		
		String fqdn = nsUtils.getNSFromString(resource);

		if (type.equals(LODVaderProperties.TYPE_SUBJECT)) {
			DBCollection collection = DBSuperClass.getInstance()
					.getCollection(DistributionSubjectNSDB.COLLECTION_NAME);

			DBObject query;
			query = new BasicDBObject(DistributionSubjectNSDB.SUBJECT_NS, fqdn);

			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				DistributionDB d = new DistributionDB(
						Integer.valueOf(instance.get(DistributionSubjectNSDB.DISTRIBUTION_ID).toString()));
				
				if(LoadedBloomFiltersCache.querySubject(d, resource))
					setOfDistributionFQDN.add(d);
			}
		}
		
		else if (type.equals(LODVaderProperties.TYPE_OBJECT)) {
			DBCollection collection = DBSuperClass.getInstance()
					.getCollection(DistributionObjectNSDB.COLLECTION_NAME);

			DBObject query;
			query = new BasicDBObject(DistributionObjectNSDB.OBJECT_NS, fqdn);

			DBCursor instances = collection.find(query);
			
			for (DBObject instance : instances) {
				DistributionDB d = new DistributionDB(
						Integer.valueOf(instance.get(DistributionObjectNSDB.DISTRIBUTION_ID).toString()));
				
				if(LoadedBloomFiltersCache.queryObject(d, resource))
					setOfDistributionFQDN.add(d);
			}
		}
		
		else if (type.equals(LODVaderProperties.TYPE_PROPERTY)) {
			DBCollection collection = DBSuperClass.getInstance()
					.getCollection(AllPredicatesDB.COLLECTION_NAME);

			DBObject query;
			query = new BasicDBObject(AllPredicatesDB.URI, resource);

			DBCursor instances = collection.find(query);
			
			int predicateID ;
			
			if(instances.iterator().hasNext())
				predicateID = Integer.parseInt(instances.iterator().next().get(new AllPredicatesDB().LOD_VADER_ID).toString()); 
			else 
				return setOfDistributionFQDN;
			
			collection = DBSuperClass.getInstance()
					.getCollection(AllPredicatesRelationDB.COLLECTION_NAME);
			query = new BasicDBObject(AllPredicatesRelationDB.PREDICATE_ID, predicateID);

			instances = collection.find(query);
			
			for (DBObject instance : instances) {
				DistributionDB d = new DistributionDB(
						Integer.valueOf(instance.get(AllPredicatesRelationDB.DISTRIBUTION_ID).toString()));
					setOfDistributionFQDN.add(d);
			}
		}
			
	
	return setOfDistributionFQDN;
	
	}

}
