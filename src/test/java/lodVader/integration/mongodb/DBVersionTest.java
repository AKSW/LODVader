package lodVader.integration.mongodb;

import org.junit.Test;

import junit.framework.Assert;
import lodVader.LODVaderProperties;
import lodVader.mongodb.DBSuperClass;

public class DBVersionTest {

	@Test
	public void versionTest() {
		Assert.assertTrue(DBSuperClass.getInstance().getMongo().MAJOR_VERSION >= LODVaderProperties.MONGODB_MINIMUM_MAJOR_VERSION);
		Assert.assertTrue(DBSuperClass.getInstance().getMongo().MINOR_VERSION >= LODVaderProperties.MONGODB_MINIMUM_MINOR_VERSION);
	}

}
