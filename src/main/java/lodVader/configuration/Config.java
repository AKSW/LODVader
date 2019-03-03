package lodVader.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.data.mongodb.MongoDbFactory;

import lodVader.mongodb.DBSuperClass2;
import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LODVaderCounterDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.SystemPropertiesDB;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
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

	@Bean
	@Scope(value = "prototype")	
	public AllPredicatesDB getAllPredicatesDB() {
		return new AllPredicatesDB(getDBSuperClass2());
	}	
	
	@Bean
	@Scope(value = "prototype")	
	public AllPredicatesRelationDB getAllPredicatesRelationDB() {
		return new AllPredicatesRelationDB(getDBSuperClass2());
	}	

	@Bean
	@Scope(value = "prototype")	
	public GeneralRDFResourceRelationDB getGeneralRDFResourceRelationDB() {
		return new GeneralRDFResourceRelationDB(getDBSuperClass2());
	}	
	
	@Bean
	@Scope(value = "prototype")	
	public RDFTypeObjectDB getRDFTypeObjectDB() {
		return new RDFTypeObjectDB(getDBSuperClass2());
	}	
		
	@Bean
	@Scope(value = "prototype")	
	public RDFTypeObjectRelationDB getRDFTypeObjectRelationDB() {
		return new RDFTypeObjectRelationDB(getDBSuperClass2());
	}	
		
	@Bean
	@Scope(value = "prototype")	
	public RDFSubClassOfDB getRDFSubClassOfDB() {
		return new RDFSubClassOfDB(getDBSuperClass2());
	}		
	
	@Bean
	@Scope(value = "prototype")	
	public RDFSubClassOfRelationDB getRDFSubClassOfRelationDB() {
		return new RDFSubClassOfRelationDB(getDBSuperClass2());
	}	
	
	@Bean
	@Scope(value = "prototype")	
	public LinksetDB getLinksetDBB() {
		return new LinksetDB(getDBSuperClass2());
	}	
	
	@Bean
	@Scope(value = "prototype")	
	public OwlClassDB getOwlClassDB() {
		return new OwlClassDB(getDBSuperClass2());
	}	
		
	
}
