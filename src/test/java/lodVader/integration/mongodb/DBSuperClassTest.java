package lodVader.integration.mongodb;

import org.junit.Test;

import lodVader.mongodb.DBSuperClass;

public class DBSuperClassTest {

	@Test
	public void testMongoDBDatabase(){
		DBSuperClass.getInstance().getCollection("SomeColection");
		DBSuperClass.getInstance().getCollection("SomeColection").drop();
	}
	
}
