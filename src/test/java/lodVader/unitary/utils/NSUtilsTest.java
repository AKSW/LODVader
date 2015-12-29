package lodVader.unitary.utils;

import org.junit.Assert;
import org.junit.Test;

import lodVader.utils.NSUtils;
import lodVader.utils.Timer;

public class NSUtilsTest {

	@Test
	public void testANS() {
		NSUtils nsUtils = new NSUtils();
		String url = "http://lodvader.aksw.org/ns/ns/ns/ns/ns/ns/ns/ns/ns/ns/ns/ns/ns/ns/ns/ns/#LOD";
		String expectedNS = "http://lodvader.aksw.org/ns1/#";

		Timer t = new Timer();
		t.startTimer();
		int i;
		for (i = 0; i < 10000; i++) {
			nsUtils.getNSFromString1(url);			
		}		
		System.out.println(t.stopTimer());
		t = new Timer();
		t.startTimer();

		for (i = 0; i < 10000; i++) {
			nsUtils.getNSFromString(url);			
		}
		System.out.println(nsUtils.getNSFromString(url));
		System.out.println(t.stopTimer());
		
		url = "http://lodvader.aksw.org/ns1/LOD/test";
		expectedNS = "http://lodvader.aksw.org/ns1/LOD/";

		Assert.assertEquals(expectedNS, nsUtils.getNSFromString(url));

	}
}
