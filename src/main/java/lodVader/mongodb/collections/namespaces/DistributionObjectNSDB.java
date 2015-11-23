package lodVader.mongodb.collections.namespaces;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.mongodb.BasicDBObject;

import lodVader.LoadedBloomFiltersCache;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.DistributionDB;

public class DistributionObjectNSDB extends SuperNS {

	public static final String NUMBER_OF_RESOURCES = "numberOfResources";
	public static String COLLECTION_NAME = "DistributionObjectNS";

	public DistributionObjectNSDB() {
		super.COLLECTION_NAME = DistributionObjectNSDB.COLLECTION_NAME;
	}

	public int getNumberOfResources() {
		return ((Number) getField(NUMBER_OF_RESOURCES)).intValue();
	}

	public void setNumberOfResources(int numberOfResources) {
		addField(NUMBER_OF_RESOURCES, numberOfResources);
	}
	
	public void bulkSave(ConcurrentHashMap<String, Integer> map, DistributionDB distribution) {
		Iterator it = map.entrySet().iterator();
		getCollection().remove(new BasicDBObject(DISTRIBUTION_ID, distribution.getLODVaderID()));
		DistributionObjectNSDB d2 = null;
		
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String d = (String) pair.getKey();
			int count = (Integer) pair.getValue();
			d2 = new DistributionObjectNSDB();
			d2.setNS(d);
			d2.setNumberOfResources(count);
			d2.setDatasetID(distribution.getTopDatasetID());
			d2.setDistributionID(distribution.getLODVaderID());
			
			// add to current BF
			if(!LoadedBloomFiltersCache.describedObjectsNS.compare(d)){
				LoadedBloomFiltersCache.describedObjectsNS.add(d);
				LoadedBloomFiltersCache.describedObjectsNSCurrentSize ++;
			}

			try {
				d2.updateObject(true);
			} catch (LODVaderMissingPropertiesException e) {
				e.printStackTrace();
			}
		}
	}

}
 