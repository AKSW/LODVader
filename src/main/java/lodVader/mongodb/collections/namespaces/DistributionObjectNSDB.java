package lodVader.mongodb.collections.namespaces;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.utils.NSUtils;

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
		NSUtils nsUtils = new NSUtils();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String d = (String) pair.getKey();
			int count = (Integer) pair.getValue();
			DistributionObjectNSDB d2 = new DistributionObjectNSDB();
			d2.setNS(d);
			d2.setNumberOfResources(count);
			d2.setDatasetID(distribution.getTopDatasetID());
			d2.setDistributionID(distribution.getLODVaderID());

			try {
				d2.updateObject(true);
			} catch (LODVaderMissingPropertiesException e) {
				e.printStackTrace();
			}
		}
	}

}
 