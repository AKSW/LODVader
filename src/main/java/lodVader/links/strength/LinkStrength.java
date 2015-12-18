package lodVader.links.strength;

import java.util.ArrayList;

import org.junit.Test;

import lodVader.exceptions.LODVaderMissingPropertiesException;
import lodVader.exceptions.mongodb.LODVaderNoPKFoundException;
import lodVader.exceptions.mongodb.LODVaderObjectAlreadyExistsException;
import lodVader.mongodb.collections.DistributionDB;
import lodVader.mongodb.collections.LinksetDB;
import lodVader.mongodb.queries.DistributionQueries;
import lodVader.mongodb.queries.NSQueries;

public class LinkStrength {
	
	@Test
	public void Go(){
		//8996-31170
		DistributionDB d = new DistributionDB(49);
//		System.out.println(d.getUri());
		updateLinks(d);
//		System.out.println(new FQDNQueries().getNumberOfObjectResources(66));
	}
	
	/**
	 * Update values of distribution similarities 
	 * @param distribution Distribution that should be compared
	 */
	public void updateLinks(DistributionDB distribution){
		
		// get all distributions except for the current one
		ArrayList<DistributionDB> distributions = new DistributionQueries().getDistributions(null, null);
		
		for(DistributionDB d: distributions){
			if(d.getLODVaderID() != distribution.getLODVaderID()){
				makeLink(distribution, d);
				makeLink(d, distribution);
				
			}
		}
	}
	/**
	 * Update link similarity value at mongodb
	 * @param dist1 distribution 1
	 * @param dist2 distribution 2
	 * @param value similarity value
	 */
	private void makeLink(DistributionDB dist1, DistributionDB dist2){
		String id = String.valueOf(dist1.getLODVaderID()) + "-" + String.valueOf(dist2.getLODVaderID());		
		
		double nLinks = 0.0;
		int numberOfSourceFQDN = new NSQueries().getNumberOfObjectResources(dist1.getLODVaderID());
		LinksetDB link = new LinksetDB(id);
		
					
		
		if (numberOfSourceFQDN>0){
			nLinks = 1.0*link.getLinks()/numberOfSourceFQDN;
		}
		
		if (link.getLinks() == 0 &&
				link.getOwlClassSimilarity() == 0 &&
				link.getRdfSubClassSimilarity()== 0 &&
				link.getRdfTypeSimilarity()== 0
				)
			return;
		
		link.setStrength(nLinks);
		if(link.getDatasetSource()==0)
			link.setDatasetSource(dist1.getTopDatasetID());
		if(link.getDatasetTarget()==0)
			link.setDatasetTarget(dist2.getTopDatasetID());
		
		if(link.getDistributionSource()==0)
			link.setDistributionSource(dist1.getLODVaderID());
		if(link.getDistributionTarget()==0)
			link.setDistributionTarget(dist2.getLODVaderID());		
		
		try {
			link.update(true);
		} catch (LODVaderMissingPropertiesException | LODVaderObjectAlreadyExistsException
				| LODVaderNoPKFoundException e) {
			e.printStackTrace();
		}
	}
	

	
	
}
