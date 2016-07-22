/**
 * 
 */
package ldlex.seeder;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Class that maps namespaces and distributions
 * 
 * @author Ciro Baron Neto
 * 
 *         Jul 21, 2016
 */
public class NSDistributionMapperHashImpl implements NSDistributionMapperInterface {

	private HashMap<String, HashSet<Integer>> nsToDistributionMap = new HashMap<String, HashSet<Integer>>();

	public Boolean addDistribution(HashSet<String> nsSet, Integer distributionID) {

		if (nsSet == null)
			return null;
		if (distributionID == null)
			return null;

		for (String ns : nsSet) {
			HashSet<Integer> distributions = nsToDistributionMap.get(ns);
			if (distributions == null) {
				distributions = new HashSet<Integer>();
				distributions.add(distributionID);
				nsToDistributionMap.put(ns, distributions);
			} else {
				distributions.add(distributionID);
			}
		}

		return true;
	}

	public HashSet<Integer> getDistributions(String ns) {
		HashSet<Integer> map = nsToDistributionMap.get(ns);
		if (map == null)
			return new HashSet<Integer>();
		else
			return map;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * ldlex.seeder.NSDistributionMapperInterface#addDistribution(java.lang.
	 * String, java.lang.Integer)
	 */
	@Override
	public Boolean addDistribution(String ns, Integer distributionID) {
		if (ns == null)
			return null;

		HashSet<Integer> distributions = nsToDistributionMap.get(ns);
		if (distributions == null) {
			distributions = new HashSet<Integer>();
			if (distributionID != null)
				distributions.add(distributionID);
			nsToDistributionMap.put(ns, distributions);
		} else {
			if (distributionID != null)
				distributions.add(distributionID);
		}

		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ldlex.seeder.NSDistributionMapperInterface#hasNS(java.lang.String)
	 */
	@Override
	public Boolean hasNS(String ns) {
		// TODO Auto-generated method stub
		return nsToDistributionMap.containsKey(ns);
	}

}
