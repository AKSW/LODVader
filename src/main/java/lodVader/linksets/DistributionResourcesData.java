package lodVader.linksets;

import java.util.TreeMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.gridFS.ObjectsBucket;
import lodVader.mongodb.collections.gridFS.SubjectsBucket;
import lodVader.mongodb.collections.namespaces.DistributionObjectNSDB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;

public class DistributionResourcesData {

	public int distributionID;

	public DistributionDB distributionMongoDBObject;

	// public HashSet<String> subjectsNS = new HashSet<String>();
	//
	// public HashSet<String> objectsNS = new HashSet<String>();

	public GoogleBloomFilter filterSubjectsNS;

	public GoogleBloomFilter filterObjectsNS;

	protected GoogleBloomFilter singleSubject = null;

	protected GoogleBloomFilter singleObject = null;

	public TreeMap<String, ObjectsBucket> objectBuckets = new TreeMap<String, ObjectsBucket>();

	public TreeMap<String, SubjectsBucket> subjectBuckets = new TreeMap<String, SubjectsBucket>();

	public DistributionResourcesData(int distributionID) {
		this.distributionMongoDBObject = new DistributionDB(distributionID);
		this.distributionID = distributionID;
		loadObjectBuckets();
		loadSubjectBuckets();
		loadNamespaces();
	}

	public boolean querySubjectNS(String fqdn) {
		return filterSubjectsNS.compare(fqdn);

	}

	public boolean queryObjectNS(String fqdn) {
		return filterObjectsNS.compare(fqdn);
	}

	// public void addSubjectsNS(HashSet<String> list) {
	// this.subjectsNS = list;
	// }
	//
	// public void addObjectsNS(HashSet<String> list) {
	// this.objectsNS = list;
	// }

	public void loadObjectBuckets() {
		this.objectBuckets = new ObjectsBucket().createAllBuckets(distributionID);
		if (this.objectBuckets.size() == 1)
			singleObject = this.objectBuckets.firstEntry().getValue().filter;
	}

	public void loadSubjectBuckets() {
		this.subjectBuckets = new SubjectsBucket().createAllBuckets(distributionID);
		if (this.subjectBuckets.size() == 1)
			singleSubject = this.subjectBuckets.firstEntry().getValue().filter;
	}

	public boolean queryObject(String resource) {
		try {
			if (singleObject != null)
				return singleObject.compare(resource);

			// else
			// for (ObjectsBucket o : objectBuckets)
			// if (o.filter.compare(resource))
			// return true;

			else if (objectBuckets.floorEntry(resource).getValue().filter.compare(resource))
				return true;

			// objectBuckets.floorKey(resource)

		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	public boolean querySubject(String resource) {
		try {
			if (singleSubject != null)
				return singleSubject.compare(resource);

			// else
			// for (SubjectsBucket o : subjectBuckets)
			// if (o.filter.compare(resource))
			// return true;

			else if (subjectBuckets.floorEntry(resource).getValue().filter.compare(resource))
				return true;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	public void loadNamespaces() {

		// query all subjects ns for the distribution
		BasicDBObject subjectQuery = new BasicDBObject(DistributionSubjectNSDB.DISTRIBUTION_ID, distributionID);

		DBCollection collection = DBSuperClass2.getCollection(DistributionSubjectNSDB.COLLECTION_NAME);

		DBCursor cursor = collection.find(subjectQuery);

		// HashSet<String> subjectsNS = new HashSet<String>();
		String resource;
		if (cursor.size() < 10000)
			filterSubjectsNS = new GoogleBloomFilter(10000, 0.0000001);
		else
			filterSubjectsNS = new GoogleBloomFilter(cursor.size(), 0.0000001);

		while (cursor.hasNext()) {
			resource = cursor.next().get(DistributionSubjectNSDB.NS).toString();
			// subjectsNS.add(resource);
			filterSubjectsNS.add(resource);
		}

		// doing the same for objects ns
		BasicDBObject objectQuery = new BasicDBObject(DistributionObjectNSDB.DISTRIBUTION_ID, distributionID);

		collection = DBSuperClass2.getCollection(DistributionObjectNSDB.COLLECTION_NAME);

		cursor = collection.find(objectQuery);

		// HashSet<String> objectsNS = new HashSet<String>();
		if (cursor.size() < 10000)
			filterObjectsNS = new GoogleBloomFilter(10000, 0.0000001);
		else
			filterObjectsNS = new GoogleBloomFilter(cursor.size(), 0.0000001);
		while (cursor.hasNext()) {
			resource = cursor.next().get(DistributionObjectNSDB.NS).toString();
			// objectsNS.add(resource);
			filterObjectsNS.add(resource);
		}

		// addObjectsNS(objectsNS);
		// addSubjectsNS(subjectsNS);
		// System.out.println("o "+objectsNS.size());
		// System.out.println("s "+subjectsNS.size());
	}

}
