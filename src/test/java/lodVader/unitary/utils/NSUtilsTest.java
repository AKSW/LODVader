package lodVader.unitary.utils;

import org.junit.Assert;
import org.junit.Test;

import lodVader.utils.NSUtils;

public class NSUtilsTest {

	@Test
	public void testANS(){
		NSUtils nsUtils = new NSUtils();
		String url = "http://lodvader.aksw.org/ns1/#LOD";
		String expectedNS = "http://lodvader.aksw.org/ns1/#";
		
		Assert.assertEquals(expectedNS, nsUtils.getNSFromString(url));
		
		url = "http://lodvader.aksw.org/ns1/LOD/test";
		expectedNS = "http://lodvader.aksw.org/ns1/LOD/";
		
		Assert.assertEquals(expectedNS, nsUtils.getNSFromString(url));
		
		
	}
}
