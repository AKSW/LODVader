package lodVader.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties
public class PathProperties {

	// define what path should be used to store files
	public String basePath;
	
	public Boolean multithread;
	
	public String lovUrl;
	
	public String removeDatasetPass;

	public Boolean resume;
	
	public Boolean resumeErrors;
	
	public Boolean compareWayAround;
	
	public int nrThreads;
	
	
	public String getRemoveDatasetPass() {
		return removeDatasetPass;
	}
	public void setRemoveDatasetPass(String removeDatasetPass) {
		this.removeDatasetPass = removeDatasetPass;
	}
	
	public int getNrThreads() {
		return nrThreads;
	}
	public void setNrThreads(int nrThreads) {
		this.nrThreads = nrThreads;
	}
	public String getBasePath() {
		return basePath;
	}
	public void setBasePath(String basePath) {
		this.basePath = basePath;
	}
	public Boolean getMultithread() {
		return multithread;
	}
	public void setMultithread(Boolean multithread) {
		this.multithread = multithread;
	}
	public String getLovUrl() {
		return lovUrl;
	}
	public void setLovUrl(String lovUrl) {
		this.lovUrl = lovUrl;
	}
	public Boolean getResume() {
		return resume;
	}
	public void setResume(Boolean resume) {
		this.resume = resume;
	}
	public Boolean getResumeErrors() {
		return resumeErrors;
	}
	public void setResumeErrors(Boolean resumeErrors) {
		this.resumeErrors = resumeErrors;
	}
	public Boolean getCompareWayAround() {
		return compareWayAround;
	}
	public void setCompareWayAround(Boolean compareWayAround) {
		this.compareWayAround = compareWayAround;
	}
	
}
