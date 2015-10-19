package lodVader.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import lodVader.mongodb.collections.DatasetDB;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.DistributionObjectNSDB;
import lodVader.mongodb.collections.DistributionSubjectNSDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeSubjectRelationDB;

public class IndexesCreator {
	
	public void createIndexes(){
		
		// indexes for DistributionObjectDomainsMongoDBObject
		addIndex(DistributionObjectNSDB.COLLECTION_NAME, DistributionObjectNSDB.DISTRIBUTION_ID, 1);
		addIndex(DistributionObjectNSDB.COLLECTION_NAME, DistributionObjectNSDB.OBJECT_NS, 1);
		
		// indexes for DistributionSubjectDomainsMongoDBObject
		addIndex(DistributionSubjectNSDB.COLLECTION_NAME, DistributionSubjectNSDB.DISTRIBUTION_ID, 1);
		addIndex(DistributionSubjectNSDB.COLLECTION_NAME, DistributionSubjectNSDB.SUBJECT_NS, 1);
		
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
				
		// indexes for predicatesresources
		addIndex(AllPredicatesRelationDB.COLLECTION_NAME, AllPredicatesRelationDB.PREDICATE_ID, 1);
		addIndex(AllPredicatesRelationDB.COLLECTION_NAME, AllPredicatesRelationDB.DISTRIBUTION_ID, 1);
		addIndex(AllPredicatesRelationDB.COLLECTION_NAME, AllPredicatesRelationDB.DATASET_ID, 1);	
				
		// indexes for predicatesresources
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
		
		
		
	}
	
	public void addIndex(String collection, String field, int value){
		DBObject indexOptions = new BasicDBObject();
		indexOptions.put(field, value);
		DBSuperClass.getInstance().getCollection(collection).createIndex(indexOptions ); 			
		
	}

}
