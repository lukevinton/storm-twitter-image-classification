package com.lukevinton.storm.imageClassifier;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.imgscalr.Scalr;
import twitter4j.Status;

import java.awt.image.BufferedImage;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;

public class ResizeImageBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input)  {
        Status tweet = (Status) input.getValueByField("tweet");
        BufferedImage img = (BufferedImage) input.getValueByField("imgBuffer");
        if(img != null) {
            BufferedImage resizedImg = Scalr.resize(img, Scalr.Method.AUTOMATIC, Scalr.Mode.FIT_EXACT, 299, 299);
            collector.emit(new Values(tweet, resizedImg));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "imgResized"));
    }
}
