package lodVader.utils;

import java.net.MalformedURLException;

public class URLUtils {
	
	public void validateURL(String url) throws MalformedURLException {
		if (!url.startsWith("http"))
			throw new MalformedURLException("Bad URL: " + url);
	}
} 
