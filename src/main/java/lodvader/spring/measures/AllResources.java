package lodVader.spring.measures;

import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

import lodVader.enumerators.TuplePart;
import lodVader.linksets.DistributionBloomFilterContainer;
import lodVader.mongodb.queries.NSQueries;
import lodVader.utils.NSUtils;

public class AllResources {

	
	TreeMap<String, HashSet<Integer>> NSTree = null;
	
	HashMap<Integer, DistributionBloomFilterContainer> mapOfDistribution = new HashMap<Integer, DistributionBloomFilterContainer>();
	
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
			DistributionBloomFilterContainer d = mapOfDistribution.get(i);
			
			if(d==null){
				d = new DistributionBloomFilterContainer(i);
				mapOfDistribution.put(i, d);
			}
			
			if(d.querySubject(query))
				return true;
			
		}
		
		return false;
	}
	
	
}
