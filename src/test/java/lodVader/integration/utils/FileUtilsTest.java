package lodVader.integration.utils;

import java.io.File;

import org.junit.Test;

import junit.framework.Assert;
import lodVader.LODVaderProperties;

public class FileUtilsTest {
	
	@Test
	public void checkFolderPermission(){
		new LODVaderProperties().loadProperties();
		File f = new File(LODVaderProperties.BASE_PATH+"/test");
		if (!f.exists())
			Assert.assertTrue(f.mkdirs());
		File f2 = new File(LODVaderProperties.BASE_PATH+"/test");
		if (!f2.exists())
			Assert.assertTrue(f2.mkdirs());
		
		Assert.assertTrue(f2.delete());
	}

}
