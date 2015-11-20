package lodVader.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeSubjectRelationDB;
import lodVader.mongodb.collections.gridFS.SuperBucket;
import lodVader.mongodb.collections.namespaces.DistributionObjectNS0DB;
import lodVader.mongodb.collections.namespaces.DistributionObjectNSDB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNS0DB;
import lodVader.mongodb.collections.namespaces.DistributionSubjectNSDB;
import lodVader.mongodb.collections.toplinks.TopInvalidLinks;
import lodVader.mongodb.collections.toplinks.TopValidLinks;

public class IndexesCreator {
	
	public void createIndexes(){
		
		// indexes for DistributionObjectDomainsMongoDBObject
		addIndex(DistributionObjectNSDB.COLLECTION_NAME, DistributionObjectNSDB.DISTRIBUTION_ID, 1); 
		addIndex(DistributionObjectNSDB.COLLECTION_NAME, DistributionObjectNSDB.NS, 1);
		
		// indexes for DistributionSubjectDomainsMongoDBObject
		addIndex(DistributionSubjectNSDB.COLLECTION_NAME, DistributionSubjectNSDB.DISTRIBUTION_ID, 1);
		addIndex(DistributionSubjectNSDB.COLLECTION_NAME, DistributionSubjectNSDB.NS, 1); 
		
		// indexes for datasets
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.PARENT_DATASETS, 1);
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.TITLE, 1);
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.LOD_VADER_ID, 1);
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.SUBSET_IDS, 1);
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.PARENT_DATASETS, 1);
		
		// indexes for distributions
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.DEFAULT_DATASETS, 1);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.DOWNLOAD_URL, 1);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.IS_VOCABULARY, 1);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.LOD_VADER_ID, 1);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.DEFAULT_DATASETS, 1);
		
		
		// indexes for linksets
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.DATASET_SOURCE, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.DATASET_TARGET, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.DISTRIBUTION_SOURCE, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.DISTRIBUTION_TARGET, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.LINK_NUMBER_LINKS, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.PREDICATE_SIMILARITY, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.RDF_TYPE_SIMILARITY, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.RDF_SUBCLASS_SIMILARITY, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.OWL_CLASS_SIMILARITY, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.LINK_STRENGHT, 1);
				
		// indexes for predicates resources
		addIndex(AllPredicatesRelationDB.COLLECTION_NAME, AllPredicatesRelationDB.PREDICATE_ID, 1);
		addIndex(AllPredicatesRelationDB.COLLECTION_NAME, AllPredicatesRelationDB.DISTRIBUTION_ID, 1);
		addIndex(AllPredicatesRelationDB.COLLECTION_NAME, AllPredicatesRelationDB.DATASET_ID, 1);	
				
		// indexes for predicates resources
		addIndex(RDFTypeObjectRelationDB.COLLECTION_NAME, RDFTypeObjectRelationDB.PREDICATE_ID, 1);
		addIndex(RDFTypeObjectRelationDB.COLLECTION_NAME, RDFTypeObjectRelationDB.DISTRIBUTION_ID, 1);
		addIndex(RDFTypeObjectRelationDB.COLLECTION_NAME, RDFTypeObjectRelationDB.DATASET_ID, 1);
		
		addIndex(RDFTypeSubjectRelationDB.COLLECTION_NAME, RDFTypeSubjectRelationDB.PREDICATE_ID, 1);
		addIndex(RDFTypeSubjectRelationDB.COLLECTION_NAME, RDFTypeSubjectRelationDB.DISTRIBUTION_ID, 1);
		addIndex(RDFTypeSubjectRelationDB.COLLECTION_NAME, RDFTypeSubjectRelationDB.DATASET_ID, 1);
		
		addIndex(RDFSubClassOfRelationDB.COLLECTION_NAME, RDFSubClassOfRelationDB.PREDICATE_ID, 1);
		addIndex(RDFSubClassOfRelationDB.COLLECTION_NAME, RDFSubClassOfRelationDB.DISTRIBUTION_ID, 1);
		addIndex(RDFSubClassOfRelationDB.COLLECTION_NAME, RDFSubClassOfRelationDB.DATASET_ID, 1);
		
		addIndex(OwlClassRelationDB.COLLECTION_NAME, OwlClassRelationDB.PREDICATE_ID, 1);
		addIndex(OwlClassRelationDB.COLLECTION_NAME, OwlClassRelationDB.DISTRIBUTION_ID, 1);
		addIndex(OwlClassRelationDB.COLLECTION_NAME, OwlClassRelationDB.DATASET_ID, 1);
		
		addIndex(DistributionSubjectNS0DB.COLLECTION_NAME, DistributionSubjectNS0DB.DISTRIBUTION_ID, 1);
		addIndex(DistributionSubjectNS0DB.COLLECTION_NAME, DistributionSubjectNS0DB.DATASET_ID, 1);
		addIndex(DistributionSubjectNS0DB.COLLECTION_NAME, DistributionSubjectNS0DB.NS, 1);
		 
		addIndex(DistributionObjectNS0DB.COLLECTION_NAME, DistributionObjectNS0DB.DISTRIBUTION_ID, 1);
		addIndex(DistributionObjectNS0DB.COLLECTION_NAME, DistributionObjectNS0DB.DATASET_ID, 1);
		addIndex(DistributionObjectNS0DB.COLLECTION_NAME, DistributionObjectNS0DB.NS, 1);
		
		addIndex(DistributionSubjectNSDB.COLLECTION_NAME, DistributionSubjectNSDB.DISTRIBUTION_ID, 1);
		addIndex(DistributionSubjectNSDB.COLLECTION_NAME, DistributionSubjectNSDB.DATASET_ID, 1);
		addIndex(DistributionSubjectNSDB.COLLECTION_NAME, DistributionSubjectNSDB.NS, 1);
		
		addIndex(DistributionObjectNSDB.COLLECTION_NAME, DistributionObjectNSDB.DISTRIBUTION_ID, 1);
		addIndex(DistributionObjectNSDB.COLLECTION_NAME, DistributionObjectNSDB.DATASET_ID, 1);
		addIndex(DistributionObjectNSDB.COLLECTION_NAME, DistributionObjectNSDB.NS, 1);
		
		addIndex(TopInvalidLinks.COLLECTION_NAME, TopInvalidLinks.SOURCE_DISTRIBUTION_ID, 1);
		addIndex(TopInvalidLinks.COLLECTION_NAME, TopInvalidLinks.TARGET_DISTRIBUTION_ID, 1);
		addIndex(TopInvalidLinks.COLLECTION_NAME, TopInvalidLinks.AMOUNT, 1);
			
		addIndex(TopValidLinks.COLLECTION_NAME, TopValidLinks.SOURCE_DISTRIBUTION_ID, 1);
		addIndex(TopValidLinks.COLLECTION_NAME, TopValidLinks.TARGET_DISTRIBUTION_ID, 1);
		addIndex(TopValidLinks.COLLECTION_NAME, TopValidLinks.AMOUNT, 1);
			
		
		// indices for gridFS
		addIndex("ObjectsBucket.files", SuperBucket.DISTRIBUTION_ID, 1);
		addIndex("ObjectsBucket.files", SuperBucket.FIRST_RESOURCE, 1);
		addIndex("ObjectsBucket.files", SuperBucket.LAST_RESOURCE, 1);

		addIndex("SubjectsBucket.files", SuperBucket.DISTRIBUTION_ID, 1);
		addIndex("SubjectsBucket.files", SuperBucket.FIRST_RESOURCE, 1);
		addIndex("SubjectsBucket.files", SuperBucket.LAST_RESOURCE, 1);
		
		

	}
	
	public void addIndex(String collection, String field, int value){
		DBObject indexOptions = new BasicDBObject();
		indexOptions.put(field, value);
		DBSuperClass.getInstance().getCollection(collection).createIndex(indexOptions ); 			
		
	}

}
