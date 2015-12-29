package lodVader.mongodb.collections.namespaces;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.mongodb.BasicDBObject;

import lodVader.LoadedBloomFiltersCache;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.collections.DistributionDB;

public class DistributionSubjectNSDB extends SuperNS {

	public static final String NUMBER_OF_RESOURCES = "numberOfResources";

	public static String COLLECTION_NAME = "DistributionSubjectNS";

	public DistributionSubjectNSDB() {
		super(COLLECTION_NAME);
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

		DistributionSubjectNSDB d2 ;
		int distributionLODVaderID = distribution.getLODVaderID();
		int topDatasetLODVaderID = distribution.getTopDatasetID();
		
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String d = (String) pair.getKey();
			int count = (Integer) pair.getValue();
			d2 = new DistributionSubjectNSDB();
			d2.setNS(d);
			d2.setNumberOfResources(count);
			d2.setDatasetID(topDatasetLODVaderID);
			d2.setDistributionID(distributionLODVaderID);
			
			// add to current BF
			if(!LoadedBloomFiltersCache.describedSubjectsNS.compare(d)){
				LoadedBloomFiltersCache.describedSubjectsNS.add(d);
				LoadedBloomFiltersCache.describedSubjectsNSCurrentSize ++;
			}

			try {
				d2.insert(true);
			} catch (LODVaderObjectAlreadyExistsException | LODVaderNoPKFoundException e) {
				e.printStackTrace();
			}
		}
	}
}
