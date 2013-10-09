package com.mcba.util;

import java.io.IOException;

import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

public class HttpUtil {

	public static Logger LOG = Logger.getLogger(HttpUtil.class);

	public static final String getResponse(String urlAndParameter) {
		 HttpClient httpclient = new DefaultHttpClient();
	        try {
	            HttpGet httpget = new HttpGet(urlAndParameter);

	            // Create a response handler
	            ResponseHandler<String> responseHandler = new BasicResponseHandler();
	            String responseBody = httpclient.execute(httpget, responseHandler);
	            return responseBody;

	        } catch (IOException e) {
	        	LOG.info(e);
				return null;
			} finally {
	            // When HttpClient instance is no longer needed,
	            // shut down the connection manager to ensure
	            // immediate deallocation of all system resources
	            httpclient.getConnectionManager().shutdown();
	        }
	}
	
	
}
