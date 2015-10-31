package lodVader.utils;

import org.junit.Test;

public class NSUtils {

	public String getNS0(String url) {
		String[] split = url.split("/");
		if (split.length > 3)
			url = split[0] + "//" + split[2] + "/";
		return url;
	}

	public String getNS1(String url) {
		String[] split = url.split("/");
		if (split.length > 4)
			url = split[0] + "//" + split[2] + "/" + split[3] + "/";
		return url;
	}

	/**
	 * Get namespace from a URL
	 * 
	 * @param url
	 *            the URL to be checked
	 * @return the namespace
	 */
	public String getNSFromString(String url) {
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
			url =  url.split("#")[0]+"#";
		}
		return url;
	}

}
