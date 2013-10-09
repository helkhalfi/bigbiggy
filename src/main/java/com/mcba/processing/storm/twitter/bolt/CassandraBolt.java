package com.mcba.processing.storm.twitter.bolt;

import java.util.Map;
import java.util.UUID;

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
import com.mcba.util.storage.CassandraUtil;


public class CassandraBolt extends BaseRichBolt {

	 private static final Logger logger = LoggerFactory.getLogger(CassandraBolt.class);

	private OutputCollector _collector;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		Status status = (Status)tuple.getValueByField(Constant.TWEET_STATUS);
		UUID uuid = (UUID)tuple.getValueByField(Constant.UUID);
		CassandraUtil.connect(PropertiesLoader.get("cluster.cassandra.node"));
		CassandraUtil.insertDataTweetStatus(uuid, status.toString());
		_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
