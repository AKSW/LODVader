package lodVader.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties
public class LodVaderPropertiesNew {
	
	
	public PathProperties pathProperties;
	

	public PathProperties getPathProperties() {
		return pathProperties;
	}
	public void setPathProperties(PathProperties pathProperties) {
		this.pathProperties = pathProperties;
	}
	
}
