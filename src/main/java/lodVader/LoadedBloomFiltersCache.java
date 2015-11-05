package lodVader;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.gridFS.ObjectsBucket;
import lodVader.mongodb.collections.gridFS.SubjectsBucket;
import lodVader.mongodb.collections.gridFS.SuperBucket;
import lodVader.utils.Timer;

public class LoadedBloomFiltersCache extends Thread{
	
	SuperBucket s;
	
	public DistributionDB distribution;
	
	String query;
	
	public boolean found;
	
	
	public LoadedBloomFiltersCache(DistributionDB distribution, String query, String type) {
		
		if(type.equals(LODVaderProperties.TYPE_PROPERTY))
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
	
}
