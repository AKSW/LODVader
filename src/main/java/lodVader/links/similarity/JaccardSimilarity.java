package lodVader.links.similarity;

import java.util.HashSet;
import java.util.Set;

/**
 * This class is a comparator class which can be used
 * to compute the jaccard coefficient
 * 
 * @author ciro
 *
 */
public class JaccardSimilarity extends LinkSimilarity {

	/**
	 * This method can be used to compute the Jaccard coefficient
	 * for two input sets.
	 * 
	 * @param s1	- first set
	 * @param s2	- second set
	 * @return jaccard coefficient of the two input sets
	 */
	@Override
	public double compare(Set<String> s1, Set<String> s2) {
		// get combined size of both sets
		double sizeAll = s1.size() + s2.size();
		
		// get union of both sets
		Set<String> unionSet = new HashSet<String>(s1);
		unionSet.retainAll(s2);
		double sizeUnion = unionSet.size();
		
		// compute coefficient
		return sizeUnion / sizeAll;
	}

}
