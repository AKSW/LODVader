package lodVader;

import java.util.HashMap;

import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.gridFS.ObjectsBucket;
import lodVader.mongodb.collections.gridFS.SubjectsBucket;
import lodVader.utils.Timer;

public class LoadedBloomFiltersCache {
	
	/**
	 * Query a the subject of a distribution
	 * @param distribution
	 * @param query
	 * @return
	 */
	public static boolean querySubject(DistributionDB distribution, String query){
		boolean contains = false;
		
		Timer t = new Timer();
		t.startTimer();
		SubjectsBucket o = new SubjectsBucket();
		o.resource=query;
		if (o.query(distribution.getLODVaderID()))
			return true;

		return contains;
	}
	
	/**
	 * Query a the object of a distribution
	 * @param distribution
	 * @param query
	 * @return
	 */	
	public static boolean queryObject(DistributionDB distribution, String query){
		boolean contains = false;
		
		ObjectsBucket o = new ObjectsBucket();
		o.resource=query;
		if (o.query(distribution.getLODVaderID()))
			return true;
	
		return contains;
	}
	
}
