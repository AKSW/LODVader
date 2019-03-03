package lodVader.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.data.mongodb.MongoDbFactory;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LODVaderCounterDB;
import lodVader.mongodb.collections.SystemPropertiesDB;
import lodVader.threads.MakeLinksetsMasterThreadLDLEx;

@Configuration
public class Config{
	
	@Bean
	public LodVaderPropertiesNew getProperties() {
		return new LodVaderPropertiesNew();
	}
	
	@Bean
	@Scope(value = "prototype")
	public DBSuperClass2 getDBSuperClass2(String collection) {
		return new DBSuperClass2(collection);
	}

	@Bean
	@Scope(value = "prototype")
	public DBSuperClass2 getDBSuperClass2() {
		return new DBSuperClass2();
	}

	public MongoDbFactory getMongoDbFactory(MongoDbFactory fac) {
		return fac;
	}
	
	@Bean
	@Scope(value = "prototype")
	public LODVaderCounterDB getLODVaderCounterDB() {
		return new LODVaderCounterDB(getDBSuperClass2());
	}
	
	@Bean
	@Scope(value = "prototype")
	public SystemPropertiesDB getSystemPropertiesDB() {
		return new SystemPropertiesDB(getDBSuperClass2());
	}
	
	@Bean
	@Scope(value = "prototype")	
	public DistributionDB getDistributionDB() {
		return new DistributionDB(getDBSuperClass2());
	}	

	@Bean
	@Scope(value = "prototype")	
	public DatasetDB getDatasetDB() {
		return new DatasetDB(getDBSuperClass2());
	}	

	@Bean
	@Scope(value = "prototype")	
	public MakeLinksetsMasterThreadLDLEx getMakeLinksetsMasterThreadLDLEx() {
		return new MakeLinksetsMasterThreadLDLEx();
	}	
	
}
