package lodVader.linksets;

import java.util.TreeMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import lodVader.bloomfilters.BloomFilterI;
import lodVader.bloomfilters.impl.BloomFilterFactory;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.gridFS.ObjectsBucket;
import lodVader.mongodb.collections.gridFS.SubjectsBucket;
import lodVader.mongodb.collections.namespaces.DistributionObjectNSDB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;

public class DistributionBloomFilterContainer {

	private int distributionID;

	private BloomFilterI filterSubjectsNS;

	private BloomFilterI filterObjectsNS;

	protected BloomFilterI singleSubject = null;

	protected BloomFilterI singleObject = null;

	private TreeMap<String, ObjectsBucket> objectBuckets = new TreeMap<String, ObjectsBucket>();

	private TreeMap<String, SubjectsBucket> subjectBuckets = new TreeMap<String, SubjectsBucket>();

	public DistributionBloomFilterContainer(int distributionID) {
		this.distributionID = distributionID;
//		loadObjectBuckets();
//		loadSubjectBuckets();
//		loadNamespaces();
	}

	public int getDistributionID() {
		return distributionID;
	}

	public TreeMap<String, ObjectsBucket> getObjectBuckets() {
		return objectBuckets;
	}

	public TreeMap<String, SubjectsBucket> getSubjectBuckets() {
		return subjectBuckets;
	}

	public BloomFilterI getFilterObjectsNS() {
		return filterObjectsNS;
	}

	public BloomFilterI getFilterSubjectsNS() {
		return filterSubjectsNS;
	}

	public boolean querySubjectNS(String fqdn) {
		return filterSubjectsNS.compare(fqdn);

	}

	public boolean queryObjectNS(String fqdn) {
		return filterObjectsNS.compare(fqdn);
	}

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

			else if (subjectBuckets.floorEntry(resource).getValue().filter.compare(resource))
				return true;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	public void loadSubjectNamespaces() {
		// query all subjects ns for the distribution
		BasicDBObject subjectQuery = new BasicDBObject(DistributionSubjectNSDB.DISTRIBUTION_ID, distributionID);

		DBCollection collection = DBSuperClass2.getCollection(DistributionSubjectNSDB.COLLECTION_NAME);

		DBCursor cursor = collection.find(subjectQuery);

		// HashSet<String> subjectsNS = new HashSet<String>();
		String resource;
		filterSubjectsNS = BloomFilterFactory.newBloomFilter();
		if (cursor.size() < 10000) {
			filterSubjectsNS.create(10000, 0.0000001);
		} else {
			filterSubjectsNS.create(cursor.size(), 0.0000001);
		}

		while (cursor.hasNext()) {
			resource = cursor.next().get(DistributionSubjectNSDB.NS).toString();
			// subjectsNS.add(resource);
			filterSubjectsNS.add(resource);
		}

	}

	public void loadObjectNamespaces() {

		// doing the same for objects ns
		BasicDBObject objectQuery = new BasicDBObject(DistributionObjectNSDB.DISTRIBUTION_ID, distributionID);

		DBCollection collection = DBSuperClass2.getCollection(DistributionObjectNSDB.COLLECTION_NAME);

		DBCursor cursor = collection.find(objectQuery);

		String resource;

		filterObjectsNS = BloomFilterFactory.newBloomFilter();
		if (cursor.size() < 10000)
			filterObjectsNS.create(10000, 0.0000001);
		else
			filterObjectsNS.create(cursor.size(), 0.0000001);
		while (cursor.hasNext()) {
			resource = cursor.next().get(DistributionObjectNSDB.NS).toString();
			// objectsNS.add(resource);
			filterObjectsNS.add(resource);
		}
	}

}
