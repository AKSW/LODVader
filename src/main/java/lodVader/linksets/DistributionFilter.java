package lodVader.linksets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import lodVader.mongodb.DBSuperClass;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.DistributionObjectNSDB;
import lodVader.mongodb.collections.DistributionSubjectNSDB;
import lodVader.mongodb.collections.gridFS.ObjectsBucket;
import lodVader.mongodb.collections.gridFS.SubjectsBucket;

public class DistributionFilter {

	public int distributionID;

	public DistributionDB distributionMongoDBObject;

	public HashSet<String> subjectsNS = new HashSet<String>();
 
	public HashSet<String> objectsNS = new HashSet<String>();

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
		return subjectsNS.contains(fqdn);
	}

	public boolean queryObjectNS(String fqdn) {
		return objectsNS.contains(fqdn);
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

		while (cursor.hasNext()) {
			subjectsNS.add(cursor.next().get(DistributionSubjectNSDB.SUBJECT_NS).toString());
		}

		// doing the same for objects ns
		BasicDBObject objectQuery = new BasicDBObject(DistributionObjectNSDB.DISTRIBUTION_ID, distributionID);

		collection = DBSuperClass.getInstance().getCollection(DistributionObjectNSDB.COLLECTION_NAME);

		cursor = collection.find(objectQuery);

		HashSet<String> objectsNS = new HashSet<String>();

		while (cursor.hasNext()) {
			objectsNS.add(cursor.next().get(DistributionObjectNSDB.OBJECT_NS).toString());
		}

		addObjectsNS(objectsNS);
		addSubjectsNS(subjectsNS);
	}

}
