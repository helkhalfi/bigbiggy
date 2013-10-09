package com.mcba.processing.storm.twitter;
import org.apache.commons.lang.StringUtils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.mcba.processing.storm.twitter.bolt.CassandraBolt;
import com.mcba.processing.storm.twitter.bolt.ElasticSearchBolt;
import com.mcba.processing.storm.twitter.bolt.GeoTweetBolt;
import com.mcba.processing.storm.twitter.bolt.TwitterStatusSysoutBolt;
import com.mcba.processing.storm.twitter.bolt.UUIDBolt;
import com.mcba.processing.storm.twitter.spout.TwitterSpout;
import com.mcba.util.storage.CassandraUtil;

/**
 * This is a basic example of a Storm topology.
 */
public class TwitterTopology {

	public static void connectToCassandra() {
		CassandraUtil.connect("127.0.0.1");
		CassandraUtil.createTwitterSchema();
	}

	public static void buildAndStartTopology(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		if(StringUtils.isEmpty(args[1])) {
			throw new IllegalArgumentException("please add at least one word to search");
		}
		String[] searchWord = StringUtils.split(args[1], ',');
		builder.setSpout("TwitterSpout", new TwitterSpout(searchWord), 1);
		builder.setBolt("UUIDBolt", new UUIDBolt(), 1).shuffleGrouping("TwitterSpout");
		builder.setBolt("TwitterSysoutBolt", new TwitterStatusSysoutBolt(), 1).shuffleGrouping("UUIDBolt");
		builder.setBolt("CassandraBolt", new CassandraBolt(), 1).shuffleGrouping("UUIDBolt");
		builder.setBolt("ElasticSearchBolt", new ElasticSearchBolt(), 1).shuffleGrouping("UUIDBolt");
		builder.setBolt("GeoTweetBolt", new GeoTweetBolt(), 1).shuffleGrouping("UUIDBolt");
		
		Config conf = new Config();
		conf.setDebug(true);
		
		StormTopology topology = builder.createTopology();

		if(args!=null && args.length == 2) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, topology);
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, topology);
			Utils.sleep(Long.MAX_VALUE);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

	public static void main(String[] args) throws Exception {
		buildAndStartTopology(args);
	}
}