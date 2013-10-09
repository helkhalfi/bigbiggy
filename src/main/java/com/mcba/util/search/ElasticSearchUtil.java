package com.mcba.util.search;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.mcba.util.properties.PropertiesLoader;

public class ElasticSearchUtil {

	private static Client client;
	private static boolean isConnected = false;
	public static synchronized Client getClient() {
		if(isConnected) {
			return client;
		}
		Settings settings = ImmutableSettings.settingsBuilder()
		        .put("cluster.name", "MCBA_SEARCH_CLUSTER").build();
		client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(PropertiesLoader.get("cluster.elasticsearch.node"), 9300));
		   //.addTransportAddress(new InetSocketTransportAddress("host2", 9300));
		isConnected = true;
		return client;
	}
	
	
}
