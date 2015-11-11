package lodVader.integration.mongodb;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ DBSuperClassTest.class, DBVersionTest.class })
public class AllTests {

}
