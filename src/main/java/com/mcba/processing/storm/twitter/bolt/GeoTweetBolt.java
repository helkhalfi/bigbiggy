package com.mcba.processing.storm.twitter.bolt;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import twitter4j.GeoLocation;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.mcba.util.Constant;
import com.mcba.util.HttpUtil;
import com.mcba.util.storage.CassandraUtil;


public class GeoTweetBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5256367253551056021L;
	public static Logger LOG = Logger.getLogger(HttpUtil.class);

	OutputCollector _collector;

	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
        UUID uuid = (UUID) tuple.getValueByField(Constant.UUID);
        Status status = (Status)tuple.getValueByField(Constant.TWEET_STATUS);
		GeoLocation geoLocation = status.getGeoLocation();
		double latitule = Constant.UNKNOWN_LATITUDE;
		double longitde = Constant.UNKNOWN_LONGITUDE;
		String city = null;
		String country = null;
		if(geoLocation != null) {
			latitule = geoLocation.getLatitude();
			longitde = geoLocation.getLongitude();
			//https://maps.googleapis.com/maps/api/geocode/json?latlng=48.9167,2.3833&sensor=false
			String responce = HttpUtil.getResponse("https://maps.googleapis.com/maps/api/geocode/json?latlng=" + latitule + "," + longitde + "&sensor=false");
			if(!StringUtils.isEmpty(responce)) {
				JSONParser parser = new JSONParser();
				try {
					JSONObject jsonObject = (JSONObject)parser.parse(responce);
					JSONArray results = (JSONArray) jsonObject.get("results");
					JSONObject address_components = (JSONObject)results.get(0);
					JSONArray address_componentsArray = (JSONArray)address_components.get("address_components");
					JSONObject addressCity = (JSONObject) address_componentsArray.get(2);
					city = (String)addressCity.get("long_name");
					JSONObject addressCountry = (JSONObject) address_componentsArray.get(5);
					country = (String)addressCountry.get("long_name");
                    
				} catch (Exception e) {
					LOG.info(e);
				}
			}
		}
		CassandraUtil.insertDataTweetGeo(uuid, latitule, longitde, city, country);
		_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
