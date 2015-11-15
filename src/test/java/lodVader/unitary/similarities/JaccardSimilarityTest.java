package lodVader.unitary.similarities;

import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

import lodVader.links.similarity.JaccardSimilarity;

public class JaccardSimilarityTest {

	@Test
	public void jaccardSimilarity(){
		TreeSet<String> set1 = new TreeSet<String>();
		set1.add("1");
		set1.add("2");
		set1.add("3");	
		
		TreeSet<String> set2 = new TreeSet<String>();
		set2.add("1");
		set2.add("2");
		set2.add("3");	
		
		JaccardSimilarity j = new JaccardSimilarity();

		Assert.assertTrue(j.compare(set1, set2) == (double) 1);
		
	}
	
}
