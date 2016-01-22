package lodVader.bloomfilters.models;

import lodVader.LODVaderProperties;
import lodVader.bloomfilters.GoogleBloomFilter;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.gridFS.ObjectsBucket;
import lodVader.mongodb.collections.gridFS.SubjectsBucket;
import lodVader.mongodb.collections.gridFS.SuperBucket;

public class LoadedBloomFiltersCache extends Thread{
	
	private SuperBucket s;
	
	private DistributionDB distribution;
	
	String query;
	
	public boolean found;
	
	
	public static GoogleBloomFilter describedSubjectsNS = null;
	public static GoogleBloomFilter describedObjectsNS = null;
	
	
	public static int describedSubjectsNSCurrentSize = 0;
	public static int describedObjectsNSCurrentSize = 0;
	
	
	public LoadedBloomFiltersCache(DistributionDB distribution, String query, String type) {
		
		if(type.equals(LODVaderProperties.TYPE_SUBJECT))
			s = new SubjectsBucket();
		else
			s = new ObjectsBucket();	
		this.query = query;
		this.distribution = distribution;
	}
	
	@Override
	public void run() {
		s.resource=query;
		if (s.query(distribution.getLODVaderID())) 
			found= true;
	}
	
	public void setDistribution(DistributionDB distribution) {
		this.distribution = distribution;
	}
	
	public DistributionDB getDistribution() {
		return distribution;
	}
	
}
