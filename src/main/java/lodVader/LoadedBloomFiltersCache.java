package lodVader;

import java.util.HashMap;

import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.collections.DistributionDB;

public class LoadedBloomFiltersCache {

	public static HashMap<String, GoogleBloomFilter> subjectFilters = new HashMap<String, GoogleBloomFilter>(); 
	
	public static HashMap<String, GoogleBloomFilter> objectFilters = new HashMap<String, GoogleBloomFilter>(); 
	
	/**
	 * Load a subject filter
	 * @param distribution
	 */
	private static synchronized void loadSubjectFilter(DistributionDB distribution){
		if(!subjectFilters.containsKey(distribution.getUri())){
			GoogleBloomFilter f = new GoogleBloomFilter();
			f.loadFilter(distribution.getSubjectFilterPath(), distribution.getTitle());
			subjectFilters.put(distribution.getUri(), f);
		}
	}
	
	/**
	 * Load a object filter
	 * @param distribution
	 */
	private static synchronized void loadObjectFilter(DistributionDB distribution){
		if(!objectFilters.containsKey(distribution.getUri())){
			GoogleBloomFilter f = new GoogleBloomFilter();
			f.loadFilter(distribution.getObjectFilterPath(), distribution.getTitle());
			objectFilters.put(distribution.getUri(), f);
		}
	}
	
	/**
	 * Query a the subject of a distribution
	 * @param distribution
	 * @param query
	 * @return
	 */
	public static boolean querySubject(DistributionDB distribution, String query){
		boolean contains = false;
		
		loadSubjectFilter(distribution);
		
		GoogleBloomFilter f = subjectFilters.get(distribution.getUri());
		try {
			if(f.compare(query))
				return true; 
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		
		loadObjectFilter(distribution);
		
		GoogleBloomFilter f = objectFilters.get(distribution.getUri());
		try {
			if(f.compare(query))
				return true; 
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return contains;
	}
	
}
