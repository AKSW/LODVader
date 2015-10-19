package lodVader.linksets;

import java.util.TreeSet;

import lodVader.mongodb.collections.DistributionDB;

public class DistributionNS {
	
	public int distribution;

	public DistributionDB distributionMongoDBObject;
	
	public TreeSet<String> subjectsFQDN = new TreeSet<String>();
	
	public TreeSet<String> objectsFQDN = new TreeSet<String>();
	
	
	public boolean hasSubjectNS(String fqdn){
		return subjectsFQDN.contains(fqdn);
	}
	
	public boolean hasObjectNS(String fqdn){
		return objectsFQDN.contains(fqdn);
	}
	
	public void addSubjectsFQDN(TreeSet<String> list){
		this.subjectsFQDN = list;
	}
	
	public void addObjectsFQDN(TreeSet<String> list){
		this.objectsFQDN = list;
	}
	
	
}
