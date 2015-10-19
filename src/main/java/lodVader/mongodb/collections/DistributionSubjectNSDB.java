package lodVader.mongodb.collections;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import lodVader.exceptions.LODVaderLODGeneralException;
import lodVader.mongodb.DBSuperClass;

public class DistributionSubjectNSDB extends DBSuperClass {
	// Collection name
		public static final String COLLECTION_NAME = "DistributionSubjectNS";

		
		// class properties
		public static final String DISTRIBUTION_ID = "distributionID";
		
		public static final String SUBJECT_NS = "subjectNS";	
		
		public static final String NUMBER_OF_RESOURCES = "numberOfResources";	
		
		
		private int distributionID;

		private String subjectNS;
		
		private int numberOfResources;
		
		
		public DistributionSubjectNSDB(String uri) {
			
			super(COLLECTION_NAME, uri);
			loadObject();
		}

		public boolean updateObject(boolean checkBeforeInsert) throws LODVaderLODGeneralException {

			BasicDBObject mongoDBObject2 = new BasicDBObject();
			
			// save object case it doesn't exists
			try {
				// updating subjectsTarget on mongodb
				mongoDBObject.put(DISTRIBUTION_ID, distributionID);
				mongoDBObject2.put(DISTRIBUTION_ID, distributionID);

				// updating objectsTarget on mongodb
				mongoDBObject.put(SUBJECT_NS, subjectNS);
				mongoDBObject2.put(SUBJECT_NS, subjectNS);

				// updating number of resources on mongodb
				mongoDBObject.put(NUMBER_OF_RESOURCES, numberOfResources);
				mongoDBObject2.put(NUMBER_OF_RESOURCES, numberOfResources);

				
				DBCursor d = objectCollection.find(mongoDBObject2);
				if (d.hasNext())
					return false;
				
				insert(checkBeforeInsert);
			} catch (Exception e2) {
				// e2.printStackTrace();

				try {
					if (update())
						return true;
					else
						return false;
				} catch (LODVaderLODGeneralException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}
			}
			return false;
		}

		protected boolean loadObject() {
			DBObject obj = search();

			if (obj != null) {

				distributionID = ((Number) obj.get(DISTRIBUTION_ID)).intValue();

				numberOfResources = ((Number) obj.get(NUMBER_OF_RESOURCES)).intValue();

				subjectNS = (String) obj.get(SUBJECT_NS);

				return true;
			}
			return false;
		}
		

		public int getDistributionID() {
			return distributionID;
		}

		public void setDistributionID(int distributionID) {
			this.distributionID = distributionID;
		}

		public String getSubjectFQDN() {
			return subjectNS;
		}

		public void setSubjectFQDN(String subjectFQDN) {
			this.subjectNS = subjectFQDN;
		}

		public int getNumberOfResources() {
			return numberOfResources;
		}

		public void setNumberOfResources(int numberOfResources) {
			this.numberOfResources = numberOfResources;
		}

	
		
}
