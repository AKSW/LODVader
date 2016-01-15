package lodVader.utils;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class NSUtils {

	public String getNS0(String url) {
		String[] split = url.split("/");
		if (split.length > 3)
			url = split[0] + "//" + split[2] + "/";
		else if (!url.endsWith("/"))
			url = url + "/";
		return url;
	}
	

	public String getNS1(String url) {
		String[] split = url.split("/");
		if (split.length > 4)
			url = split[0] + "//" + split[2] + "/" + split[3] + "/";
		return url;
	}
	
	public String getNSFromString(String url) {

		String[] split =url.split("/");
		int total = split.length;
		
		if (total <= 7) {
			int index = url.lastIndexOf("#");
			if (index == -1)
				index = url.lastIndexOf("/");

			return url.substring(0, index + 1);
		}
		else{
			int index = StringUtils.ordinalIndexOf(url, "/", 7);
			return url.substring(0, index + 1); 
		}
	}
	
	@Test
	public void oi(){
		System.out.println(getNSFromString("http://www.ciro.com////", 1));
	}
	
	public String getNSFromString(String url, int nsLevel) {
		nsLevel = nsLevel + 3;
		String[] split =url.split("/");
		int total = split.length;
		
		if (total <= nsLevel) {
			int index = url.lastIndexOf("#");
			if (index == -1)
				index = url.lastIndexOf("/");

			return url.substring(0, index + 1);
		}
		else{
			int index = StringUtils.ordinalIndexOf(url, "/", nsLevel);
			return url.substring(0, index + 1); 
		}
	}

	/**
	 * Get namespace from a URL
	 * 
	 * @param url
	 *            the URL to be checked
	 * @return the namespace
	 */
	public String getNSFromString1(String url) {
		try {
			String[] split = url.split("/");

			if (split.length > 7) {
				if (split[7].contains("#"))
					url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/" + split[5] + "/" + split[6]
							+ "/" + split[7].split("#")[0] + "#";
				else
					url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/" + split[5] + "/" + split[6]
							+ "/";
			}

			else if (split.length > 6) {
				if (split[6].contains("#"))
					url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/" + split[5] + "/"
							+ split[6].split("#")[0] + "#";
				else
					url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/" + split[5] + "/";
			}

			else if (split.length > 5) {
				if (split[5].contains("#"))
					url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/" + split[5].split("#")[0]
							+ "#";
				else
					url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4] + "/";
			}

			else if (split.length > 4) {
				if (split[4].contains("#"))
					url = split[0] + "//" + split[2] + "/" + split[3] + "/" + split[4].split("#")[0] + "#";
				else
					url = split[0] + "//" + split[2] + "/" + split[3] + "/";
			}

			else if (split.length > 3) {
				if (split[3].contains("#"))
					url = split[0] + "//" + split[2] + "/" + split[3].split("#")[0] + "#";
				else
					url = split[0] + "//" + split[2] + "/";
			} else {
				url = "";
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			url = url.split("#")[0] + "#";
		}
		return url;
	}

}
