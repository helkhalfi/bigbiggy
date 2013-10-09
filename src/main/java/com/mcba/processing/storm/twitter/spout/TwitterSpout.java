package com.mcba.processing.storm.twitter.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import twitter4j.Status;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.mcba.social.twitter.BasicUserStreamListener;
import com.mcba.util.Constant;

public class TwitterSpout extends BaseRichSpout {

	private static final long serialVersionUID = 5548655671442612224L;
	public static Logger LOG = Logger.getLogger(TestWordSpout.class);
	private boolean _isDistributed;
	private SpoutOutputCollector _collector;
	private LinkedBlockingQueue<Status> queue = null;
	private String[] searchWord = null;
	
	public TwitterSpout(String[] searchWord) {
		this.searchWord = searchWord;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		queue = new LinkedBlockingQueue<Status>(1000);
		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();

		BasicUserStreamListener basicUserStreamListener = new BasicUserStreamListener() {
			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}
		};

		twitterStream.addListener(basicUserStreamListener);
		twitterStream.user(searchWord);
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if(ret==null) {
			Utils.sleep(50);
		} else {
			_collector.emit(new Values(ret));
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Constant.TWEET_STATUS));
	}

}
