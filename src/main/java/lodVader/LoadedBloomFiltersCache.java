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
		Timer t = new Timer();
		t.startTimer();
		s.resource=query;
		if (s.query(distribution.getLODVaderID()))
			found= true;
	}
	 
	
//	/**
//	 * Query a the subject of a distribution
//	 * @param distribution
//	 * @param query
//	 * @return
//	 */
//	public static boolean querySubject(DistributionDB distribution, String query){
//	}
//	
//	public static boolean queryObject(DistributionDB distribution, String query){
//		boolean contains = false;
//		
//		ObjectsBucket o = new ObjectsBucket();
//		o.resource=query;
//		if (o.query(distribution.getLODVaderID()))
//			return true;
//	
//		return contains;
//	}
	
}
