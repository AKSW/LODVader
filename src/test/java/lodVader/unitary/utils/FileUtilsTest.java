package lodVader.unitary.utils;

import org.junit.Assert;
import org.junit.Test;

import lodVader.utils.FileUtils;

public class FileUtilsTest {

	@Test
	public void getASCIIFormatTest(){
		String str = "DBpedia Core_1";
		String expected = "DBpediaCore1";
		Assert.assertEquals(expected, FileUtils.getASCIIFormat(str));
	}
	
}
