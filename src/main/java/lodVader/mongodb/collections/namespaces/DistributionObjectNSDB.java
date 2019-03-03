package lodVader.mongodb.collections.namespaces;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;

import com.mongodb.BasicDBObject;

import lodVader.bloomfilters.models.LoadedBloomFiltersCache;
import lodVader.configuration.Config;
import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DistributionDB;

public class DistributionObjectNSDB extends SuperNS {
	
	@Autowired
	Config conf;

	public static final String NUMBER_OF_RESOURCES = "numberOfResources";
	public static String COLLECTION_NAME = "DistributionObjectNS";

	
	public DistributionObjectNSDB(DBSuperClass2 db) {
		super(db);
	}

	public int getNumberOfResources() {
		return ((Number) db.getField(NUMBER_OF_RESOURCES)).intValue();
	}

	public void setNumberOfResources(int numberOfResources) {
		db.addField(NUMBER_OF_RESOURCES, numberOfResources);
	}
	
	public void bulkSave(ConcurrentHashMap<String, Integer> map, DistributionDB distribution) {
		Iterator it = map.entrySet().iterator();
		db.getCollection().remove(new BasicDBObject(DISTRIBUTION_ID, distribution.getLODVaderID()));
		DistributionObjectNSDB d2 = null;
		
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String d = (String) pair.getKey();
			int count = (Integer) pair.getValue();
			d2 = conf.getDistributionObjectNSDB();
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
				try {
					d2.db.update(true);
				} catch (LODVaderObjectAlreadyExistsException | LODVaderNoPKFoundException e) {
					e.printStackTrace();
				}
			} catch (LODVaderMissingPropertiesException e) {
				e.printStackTrace();
			}
		}
	}

}
 