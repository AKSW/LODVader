package lodVader.links.similarity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;
import lodVader.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import lodVader.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import lodVader.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.mongodb.queries.PredicatesQueries;

public abstract class LinkSimilarity {
	
	public abstract double compare(Set<String> set1, Set<String> set2);
	
	GeneralRDFResourceRelationDB type;
	
	/**
	 * Update values of distribution similarities 
	 * @param distributionSource Distribution that should be compared
	 */
	public void updateLinks(DistributionDB distributionSource, GeneralRDFResourceRelationDB type ){
		this.type = type;
		
		// get all distributions except for the current one
		ArrayList<DistributionDB> distributions = new DistributionQueries().getDistributions(true);
		
//		PredicatesQueries predicates = new PredicatesQueries();
		HashSet<String> set1 ;
//			set1 = predicates.getSetOfPredicates(distribution.getDynLodID());
		if(type instanceof AllPredicatesRelationDB)
			set1 = new AllPredicatesRelationDB().getSetOfPredicates(distributionSource.getLODVaderID());
		else if(type instanceof RDFTypeObjectRelationDB)
			set1 = new RDFTypeObjectRelationDB().getSetOfPredicates(distributionSource.getLODVaderID());
		else if(type instanceof RDFSubClassOfRelationDB)
			set1 = new RDFSubClassOfRelationDB().getSetOfPredicates(distributionSource.getLODVaderID());
//		else if(type instanceof OwlClassRelationDB)
		else
			set1 = new OwlClassRelationDB().getSetOfPredicates(distributionSource.getLODVaderID());
		
		for(DistributionDB distributionTarget: distributions){
			if(distributionTarget.getLODVaderID() != distributionSource.getLODVaderID()){
			
			HashSet<String> set2;
			if(type instanceof AllPredicatesRelationDB)
				set2 = new AllPredicatesRelationDB().getSetOfPredicates(distributionTarget.getLODVaderID());
			else if(type instanceof RDFTypeObjectRelationDB)
				set2 = new RDFTypeObjectRelationDB().getSetOfPredicates(distributionTarget.getLODVaderID());
			else if(type instanceof RDFSubClassOfRelationDB)
				set2 = new RDFSubClassOfRelationDB().getSetOfPredicates(distributionTarget.getLODVaderID());
//			else if(type instanceof OwlClassRelationDB)
			else
				set2 = new OwlClassRelationDB().getSetOfPredicates(distributionTarget.getLODVaderID());
			
			
			double value = compare(
					set1, set2);
			
			if(value>0){
				makeLink(distributionSource, distributionTarget, value);
//				makeLink(distributionTarget, distributionSource, value);
				}
			}
		}
	}
	
	/**
	 * Update link similarity value at mongodb
	 * @param dist1 distribution 1
	 * @param dist2 distribution 2
	 * @param value similarity value
	 */
	private void makeLink(DistributionDB dist1, DistributionDB dist2, double value){
		String id = String.valueOf(dist1.getLODVaderID()) + "-" + String.valueOf(dist2.getLODVaderID());
		LinksetDB link = new LinksetDB(id);
//		link.setPredicateSimilarity(value);
		
		
		if(this.type instanceof AllPredicatesRelationDB)
			link.setPredicateSimilarity(value);
		else if(this.type instanceof RDFTypeObjectRelationDB)
			link.setRdfTypeSimilarity(value);
		else if(this.type instanceof RDFSubClassOfRelationDB)
			link.setRdfSubClassSimilarity(value);
//		else if(type instanceof OwlClassRelationDB)
		else
			link.setOwlClassSimilarity(value);
		

		if (link.getLinks() == 0 &&
				link.getOwlClassSimilarity() == 0 &&
				link.getRdfSubClassSimilarity()== 0 &&
				link.getRdfTypeSimilarity()== 0
				)
			return;
		
		if(link.getDatasetSource()==0)
			link.setDatasetSource(dist1.getTopDataset());
		if(link.getDatasetTarget()==0)
			link.setDatasetTarget(dist2.getTopDataset());
		
		if(link.getDistributionSource()==0)
			link.setDistributionSource(dist1.getLODVaderID());
		if(link.getDistributionTarget()==0)
			link.setDistributionTarget(dist2.getLODVaderID());		
		
		link.updateObject(true);
	}
}
