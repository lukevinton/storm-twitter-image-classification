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

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.util.Map;

public class SaveImageBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input)  {
        Status tweet = (Status) input.getValueByField("tweet");
        BufferedImage img = (BufferedImage) input.getValueByField("imgResized");
        String fileName = "classify" + System.currentTimeMillis() + ".jpg";
        String fileLocation = "C:\\temp\\" + fileName;
        File imgFile = new File(fileLocation);
        byte[] imgBytes = null;
        try {
            ImageIO.write(img, "jpg", imgFile);
            imgBytes = bufferToByteArray(img);
        }
        catch(IOException e) {
            fileLocation = "";
        }
        collector.emit(new Values(tweet, fileLocation, fileName,imgBytes));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "imgFileLocation", "imgFileName", "img"));
    }
    //http://www.mkyong.com/java/how-to-convert-bufferedimage-to-byte-in-java/

    private byte[] bufferToByteArray(BufferedImage img) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write( img, "jpg", baos );
        baos.flush();
        byte[] imageInByte = baos.toByteArray();
        baos.close();
        return imageInByte;
    }

}
