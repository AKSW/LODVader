package lodVader.linksets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.gridFS.ObjectsBucket;
import lodVader.mongodb.collections.gridFS.SubjectsBucket;
import lodVader.mongodb.collections.namespaces.DistributionObjectNSDB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;

public class DistributionFilter {

	public int distributionID;

	public DistributionDB distributionMongoDBObject;

	public HashSet<String> subjectsNS = new HashSet<String>();
	 
	public HashSet<String> objectsNS = new HashSet<String>();
	
	public GoogleBloomFilter filterSubjectsNS;
	 
	public GoogleBloomFilter filterObjectsNS;
	
	

	public ArrayList<ObjectsBucket> objectBuckets = new ArrayList<ObjectsBucket>();

	public ArrayList<SubjectsBucket> subjectBuckets = new ArrayList<SubjectsBucket>();
	
	public DistributionFilter(int distributionID) {
		this.distributionMongoDBObject = new DistributionDB(distributionID);
		this.distributionID = distributionID;
		loadObjectBuckets();
		loadSubjectBuckets();
		loadNS();
	}

	public boolean querySubjectNS(String fqdn) {
//		return subjectsNS.contains(fqdn);

		return filterSubjectsNS.compare(fqdn);

	}

	public boolean queryObjectNS(String fqdn) {
//		return objectsNS.contains(fqdn);
		
		return filterObjectsNS.compare(fqdn);
	}

	public void addSubjectsNS(HashSet<String> list) {
		this.subjectsNS = list;
	}

	public void addObjectsNS(HashSet<String> list) {
		this.objectsNS = list;
	}

	public void loadObjectBuckets() {
		this.objectBuckets = new ObjectsBucket().createAllBuckets(distributionID);
	}

	public void loadSubjectBuckets() {
		this.subjectBuckets = new SubjectsBucket().createAllBuckets(distributionID);
	}

	public boolean queryObject(String resources) {
		try {
			if (objectBuckets.size() == 1) {
				return objectBuckets.iterator().next().filter.compare(resources);
			}
			else for (ObjectsBucket o : objectBuckets) {
				if(o.filter.compare(resources))
					return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	public boolean querySubject(String resources) {
		try {
			if (subjectBuckets.size() == 1) {
				return subjectBuckets.iterator().next().filter.compare(resources);
			}
			else for (SubjectsBucket o : subjectBuckets) {
				if(o.filter.compare(resources))
					return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}
	
	public void loadNS(){

		// query all subjects ns for the distribution
		BasicDBObject subjectQuery = new BasicDBObject(DistributionSubjectNSDB.DISTRIBUTION_ID, distributionID);

		DBCollection collection = DBSuperClass.getInstance()
				.getCollection(DistributionSubjectNSDB.COLLECTION_NAME);

		DBCursor cursor = collection.find(subjectQuery);

		HashSet<String> subjectsNS = new HashSet<String>();
		String resource;

		filterSubjectsNS = new GoogleBloomFilter(cursor.size(),0.0000001);
		
		while (cursor.hasNext()) {
			resource = cursor.next().get(DistributionSubjectNSDB.SUBJECT_NS).toString();
			subjectsNS.add(resource);
			filterSubjectsNS.add(resource);
		}

		// doing the same for objects ns
		BasicDBObject objectQuery = new BasicDBObject(DistributionObjectNSDB.DISTRIBUTION_ID, distributionID);

		collection = DBSuperClass.getInstance().getCollection(DistributionObjectNSDB.COLLECTION_NAME);

		cursor = collection.find(objectQuery);

		HashSet<String> objectsNS = new HashSet<String>();
		filterObjectsNS = new GoogleBloomFilter(cursor.size(),0.0000001);
		while (cursor.hasNext()) {
			resource = cursor.next().get(DistributionObjectNSDB.OBJECT_NS).toString();
			objectsNS.add(resource);
			filterObjectsNS.add(resource);
		}

		addObjectsNS(objectsNS);
		addSubjectsNS(subjectsNS);
//		System.out.println("o "+objectsNS.size());
//		System.out.println("s "+subjectsNS.size());
	}

}
