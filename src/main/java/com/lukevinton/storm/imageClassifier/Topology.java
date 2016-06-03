package com.lukevinton.storm.imageClassifier;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Topology class that sets up the Storm topology for this sample.
 * Please note that Twitter credentials have to be provided as VM args, otherwise you'll get an Unauthorized error.
 * @link http://twitter4j.org/en/configuration.html#systempropertyconfiguration
 */
public class Topology {

	static final String TOPOLOGY_NAME = "storm-twitter-image-classification";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterClassifierSpout", new TwitterClassifierSpout());
		b.setBolt("DownloadImageBolt", new DownloadImageBolt()).shuffleGrouping("TwitterClassifierSpout");
		b.setBolt("ResizeImageBolt", new ResizeImageBolt()).shuffleGrouping("DownloadImageBolt");
		b.setBolt("SaveImageBolt", new SaveImageBolt()).shuffleGrouping("ResizeImageBolt");
        b.setBolt("ClassifyBolt", new ClassifyBolt()).shuffleGrouping("SaveImageBolt");
		b.setBolt("TwitterReplyBolt", new TwitterReplyBolt()).shuffleGrouping("ClassifyBolt");
		b.setBolt("HBaseBolt", new HBaseBolt()).shuffleGrouping("TwitterReplyBolt");

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});

	}

}
