package lodvader.spring.measures;

import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

import lodVader.enumerators.TuplePart;
import lodVader.linksets.DistributionResourcesData;
import lodVader.mongodb.queries.NSQueries;
import lodVader.utils.NSUtils;

public class AllResources {

	
	TreeMap<String, HashSet<Integer>> NSTree = null;
	
	HashMap<Integer, DistributionResourcesData> mapOfDistribution = new HashMap<Integer, DistributionResourcesData>();
	
	public void loadNS(){
		NSTree = new NSQueries().getNSTree(TuplePart.SUBJECT, null);
	}
	
	public void loadNS(int datasetID){
		NSTree = new NSQueries().getNSTree(TuplePart.SUBJECT, datasetID);
	}
	
	public boolean query(String query){
		
		// get set of distributions
		HashSet<Integer> distributions = NSTree.get(new NSUtils().getNSFromString(query));
		
		for(Integer i : distributions){
			
			// check whether distribution has been loaded
			DistributionResourcesData d = mapOfDistribution.get(i);
			
			if(d==null){
				d = new DistributionResourcesData(i);
				mapOfDistribution.put(i, d);
			}
			
			if(d.querySubject(query))
				return true;
			
		}
		
		return false;
	}
	
	
}
