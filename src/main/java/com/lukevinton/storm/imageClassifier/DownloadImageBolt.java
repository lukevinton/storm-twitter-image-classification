package com.lukevinton.storm.imageClassifier;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

public class DownloadImageBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input)  {
        Status tweet = (Status) input.getValueByField("tweet");
        BufferedImage img = null;
        if(tweet.getMediaEntities().length > 0) {
            String url = tweet.getMediaEntities()[0].getMediaURL() + ":small";
            try {
                img = downloadImage(url);
            }
            catch (IOException e) {
                //TODO: logging
            }
        }
        collector.emit(new Values(tweet, img));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "imgBuffer"));
    }

    private BufferedImage downloadImage(String imageUrl) throws IOException
    {
        URL url = new URL(imageUrl);
        InputStream is = url.openStream();
        return convertInputStreamToBufferedImage(is);
    }

    private BufferedImage convertInputStreamToBufferedImage(final InputStream is) throws IOException {
        BufferedImage image = null;
        if (is != null) {
            image = ImageIO.read(is);
        }
        is.close();
        return image;
    }
}
