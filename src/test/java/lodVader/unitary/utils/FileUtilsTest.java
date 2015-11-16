package lodVader.unitary.utils;

import org.junit.Assert;
import org.junit.Test;

import lodVader.utils.FileUtils;

public class FileUtilsTest {

	@Test
	public void getASCIIFormatTest(){
		String str = "LODVader @ 1 _ $ 2 3";
		String expected = "LODVader123";
		Assert.assertEquals(expected, FileUtils.getASCIIFormat(str));
	}
	
}
