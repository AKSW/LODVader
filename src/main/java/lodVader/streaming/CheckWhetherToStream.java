package lodVader.streaming;

import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.log4j.Logger;

import lodVader.mongodb.collections.DistributionDB;

public class CheckWhetherToStream {

	final static Logger logger = Logger.getLogger(CheckWhetherToStream.class);

	/**
	 * Check whether a distribution should be streamed
	 * @param distribution
	 * @return Boolean value
	 * @throws Exception
	 */
	public boolean checkDistribution(DistributionDB distribution)
			throws Exception {

		logger.debug("Checking whether we need to download "
				+ distribution.getDownloadUrl() + " again.");

		URL url = new URL(distribution.getDownloadUrl());

		logger.debug("Loading connection to: " + url.toString());
		HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
		int responseCode = httpConn.getResponseCode();

		int httpContentLength = httpConn.getContentLength();
		String httpLastModified = "";

		// check HTTP response code first
		if (responseCode == HttpURLConnection.HTTP_OK) {

			logger.debug("Connection loaded - Status: HTTP OK!");
			if (httpConn.getLastModified() > 0)
				httpLastModified = String.valueOf(httpConn.getLastModified());

			logger.debug("Old HttpByteSize: " + distribution.getHttpByteSize());
			logger.debug("Old HttpLastModified: "
					+ distribution.getHttpLastModified());
			logger.debug("New HttpByteSize: " + httpContentLength);
			logger.debug("New HttpLastModified: " + httpLastModified);

			try {
				if (distribution.getHttpByteSize().equals(
						String.valueOf(httpContentLength))
						|| distribution.getHttpLastModified().equals(
								httpLastModified)) {
					logger.info("Distribution "
							+ distribution.getDownloadUrl()
							+ " doesn't need to be downloaded again, is already in the last version.");
					return false;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		} else {
			logger.error("Connection error - Status:  " + responseCode);
		}

		logger.info("Distribution " + distribution.getDownloadUrl()
				+ " need to be downloaded.");
		return true;
	}
}
