package lodvader.unitary.filters;

import org.junit.Assert;
import org.junit.Test;

import lodVader.bloomfilters.GoogleBloomFilter;

public class BloomFilterTest {

	@Test
	public void test(){
		String str1 = "http://aksw.org/N3/News-100/96#char=950,958";
		String file = "/tmp/filter.tmp";
//		String f = "/home/ciro/LODVaderData/subjects/subject_distribution_0c7ff54b56495e96a7de02fe5c19f93e";
		double fpp = 0.0000001;
		
		GoogleBloomFilter filter = new GoogleBloomFilter(5000, fpp);
//		filter.loadFileToFilter(f);
		filter.add(str1);
		
		Assert.assertTrue(filter.compare(str1));
		
		filter.saveFilter(file);
		
		filter = new GoogleBloomFilter();
		
		filter.loadFilter(file, "ds");
		
		System.out.println(filter.compare(str1));
		
		
		
		
	}
	
}
