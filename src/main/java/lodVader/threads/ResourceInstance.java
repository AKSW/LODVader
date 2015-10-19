package lodVader.threads;

public class ResourceInstance {

	private String url;
	
	private int status;
	
	public ResourceInstance(String url, int status) {
		this.setUrl(url);
		this.setStatus(status);
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}


	
	
	
}
