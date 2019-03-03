package lodVader.mongodb.collections.namespaces;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.bloomfilters.models.LoadedBloomFiltersCache;
import lodVader.configuration.Config;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DistributionDB;

public class DistributionSubjectNSDB extends SuperNS {
	
	@Autowired
	Config conf;

	public static final String NUMBER_OF_RESOURCES = "numberOfResources";

	public static String COLLECTION_NAME = "DistributionSubjectNS";

	
	public DistributionSubjectNSDB(DBSuperClass2 db) {
		super(db);
		super.init(COLLECTION_NAME);
	}
	
	public void init(DBObject object) {
		super.db.mongoDBObject = object; 
	}

	public int getNumberOfResources() {
		return ((Number) db.getField(NUMBER_OF_RESOURCES)).intValue();
	}

	public void setNumberOfResources(int numberOfResources) {
		db.addField(NUMBER_OF_RESOURCES, numberOfResources);
	}

	public void bulkSave(ConcurrentHashMap<String, Integer> map, DistributionDB distribution) {
		
//		ArrayList<DBObject> bulkList = new ArrayList<DBObject>();
//		
//		for (String s : nsSet) {
//			mongoDBObject = new BasicDBObject();
//			setDistributionID(distributionLodVaderID);
//			setNS(s);
//			setDatasetID(topDatasetID);
//			bulkList.add(mongoDBObject);
//		}
//		
//		bulkSave2(bulkList);
		
		
		Iterator it = map.entrySet().iterator();
		db.getCollection().remove(new BasicDBObject(DISTRIBUTION_ID, distribution.getLODVaderID()));

		DistributionSubjectNSDB d2 ;
		int distributionLODVaderID = distribution.getLODVaderID();
		int topDatasetLODVaderID = distribution.getTopDatasetID();
		
		ArrayList<DBObject> bulkObjects = new ArrayList<>();
		
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String d = (String) pair.getKey();
			int count = (Integer) pair.getValue();
			d2 = conf.getDistributionSubjectNSDB();
			d2.setNS(d);
			d2.setNumberOfResources(count);
			d2.setDatasetID(topDatasetLODVaderID);
			d2.setDistributionID(distributionLODVaderID);
			
			// add to current BF
			if(!LoadedBloomFiltersCache.describedSubjectsNS.compare(d)){
				LoadedBloomFiltersCache.describedSubjectsNS.add(d);
				LoadedBloomFiltersCache.describedSubjectsNSCurrentSize ++;
			}

//			try {
				bulkObjects.add(d2.db.mongoDBObject);
//				d2.insert(true);
//			} catch (LODVaderObjectAlreadyExistsException | LODVaderNoPKFoundException e) {
//				e.printStackTrace();
//			}
				
				
		}
		db.bulkSave2(bulkObjects);
		
	}
}
