/**
 * 
 */
package ldlex.seeder;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lodVader.enumerators.TuplePart;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.utils.Timer;

/**
 * @author Ciro Baron Neto
 * 
 *         Jul 21, 2016
 */
public class MapperService {

	final static Logger logger = LoggerFactory.getLogger(MapperService.class);

	
	public void updateMapping(ArrayList<String> nsToSearch, TuplePart tuplePart,
			NSDistributionMapperInterface mapper) {
		
		logger.info("Updating mapping ("+tuplePart.toString()+") with "+nsToSearch.size()+" namespace(s)." );
		int amountOfDistributions = 0;

		if (tuplePart.equals(TuplePart.OBJECT)) {
			for (String ns : nsToSearch) {
				DistributionQueries query = new DistributionQueries();
				for (Integer distributionID : query.getDistributionsBySubjectNS(ns)) {
					mapper.addDistribution(ns, distributionID);
					amountOfDistributions++;
				}
			}

		}

		else if (tuplePart.equals(TuplePart.SUBJECT)) {
			for (String ns : nsToSearch) {
				DistributionQueries query = new DistributionQueries();
				for (Integer distributionID : query.getDistributionsByObjectNS(ns)) {
					mapper.addDistribution(ns, distributionID);
					amountOfDistributions++;
				}
			}
		}
		logger.info("Mapping ("+tuplePart.toString()+") updated with "+amountOfDistributions+" distribution(s)." );
		

	}

}
