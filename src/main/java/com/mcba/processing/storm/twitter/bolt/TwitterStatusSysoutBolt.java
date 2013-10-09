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

import com.mcba.util.Constant;

public class TwitterStatusSysoutBolt extends BaseRichBolt {

	 private static final Logger logger = LoggerFactory.getLogger(TwitterStatusSysoutBolt.class);

	private OutputCollector _collector;
	private StringBuilder buff = new StringBuilder();
	
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		 Status status = (Status)tuple.getValueByField(Constant.TWEET_STATUS);
		 UUID uuid = (UUID)tuple.getValueByField(Constant.UUID);
		 buff.append("###### Begin ######");
		 buff.append("UUID:[" + uuid.toString() + "]");
		 buff.append(status.toString());
		 buff.append("###### End ######");
		System.out.println(buff.toString());
		buff.setLength(0);
		_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}