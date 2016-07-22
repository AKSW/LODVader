/**
 * 
 */
package ldlex.seeder;

import java.util.HashMap;
import java.util.HashSet;

/**
 * @author Ciro Baron Neto
 * 
 * Jul 21, 2016
 */
public interface NSDistributionMapperInterface {
	
	/**
	 * Add new distribution to the map.
	 * 
	 * @param ns
	 *            - namespaces which the distribution holds
	 * @param distributionID
	 *            - distribution id
	 */
	public Boolean addDistribution(HashSet<String> nsSet, Integer distributionID);

	/**
	 * Query the elements for a NS
	 * @param ns
	 * @return true case the element was found
	 */
	public Boolean hasNS(String ns);

	
	/**
	 * Add a distributions and a namespace
	 * @param ns
	 * @param distributionID
	 * @return
	 */
	public Boolean addDistribution(String ns, Integer distributionID);

	
	/**
	 * Get distributions which describes certain namespace
	 * @param ns
	 * @return list of distribution ids
	 */
	public HashSet<Integer> getDistributions(String ns);
	

}
