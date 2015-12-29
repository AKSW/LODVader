package lodVader.integration.mongodb;

import org.junit.Test;

import lodVader.mongodb.DBSuperClass2;

public class DBSuperClassTest {

	@Test
	public void testMongoDBDatabase(){
		new DBSuperClass2("Collection").getDBInstance();
		new DBSuperClass2("Collection").getDBInstance().getCollection("Collection").drop();
			
	}
	
}
