package com.mcba.processing.storm.twitter.bolt;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.mcba.util.Constant;
import com.mcba.util.properties.PropertiesLoader;
import com.mcba.util.search.ElasticSearchUtil;

public class ElasticSearchBolt extends BaseRichBolt {

	 private static final Logger logger = LoggerFactory.getLogger(ElasticSearchBolt.class);

	private OutputCollector _collector;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Status status = (Status) input.getValueByField(Constant.TWEET_STATUS);
		UUID uuid = (UUID) input.getValueByField(Constant.UUID);
		Client client = ElasticSearchUtil.getClient();
		BulkRequestBuilder brb = client.prepareBulk();
		IndexRequest irq =     new IndexRequest("twitter", "tweet", uuid.toString());
		ObjectMapper mapper = new ObjectMapper();
		String jsonString = null;
		try {
			jsonString = mapper.writeValueAsString(status);
			irq.source(jsonString);
			brb.add(irq);
			BulkResponse br = brb.execute().actionGet();
		} catch ( IOException ex) {
			logger.error(ex.getMessage());
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
