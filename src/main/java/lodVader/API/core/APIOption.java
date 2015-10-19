package lodVader.API.core;

public class APIOption {
	
	public APIOption(String option, String description) {
		this.option = option;
		this.description = description;
	}

	String option; 
	
	String description;

	public String getOption() {
		return option;
	}

	public void setOption(String option) {
		this.option = option;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	} 
	
	
	
}
