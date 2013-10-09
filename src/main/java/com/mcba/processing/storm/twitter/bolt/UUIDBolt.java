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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.mcba.util.Constant;


public class UUIDBolt extends BaseRichBolt {

	private static final Logger logger = LoggerFactory.getLogger(TwitterStatusSysoutBolt.class);
	private OutputCollector _collector;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
        Status status = (Status)tuple.getValueByField(Constant.TWEET_STATUS);
		UUID id = UUID.randomUUID();
		_collector.emit(new Values(id, status));
		_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Constant.UUID, Constant.TWEET_STATUS));
	}

}
