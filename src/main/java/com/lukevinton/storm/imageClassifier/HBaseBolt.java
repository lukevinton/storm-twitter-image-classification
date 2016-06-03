package com.lukevinton.storm.imageClassifier;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import twitter4j.Status;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Map;

public class HBaseBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input)  {
        Status tweet = (Status) input.getValueByField("tweet");
        String classifications = (String) input.getValueByField("classifications");
        byte[] img= (byte[]) input.getValueByField("img");
        try {
            saveClassification(tweet,img,classifications);
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
        collector.emit(new Values());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }

    private Configuration getHBaseConfiguration()
    {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", System.getProperty("zookeeperPort"));
        conf.set("hbase.zookeeper.quorum", System.getProperty("zookeeper"));
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        return conf;
    }

    private Connection getHBaseConnection() throws IOException {
        return ConnectionFactory.createConnection(getHBaseConfiguration());
    }

    private void saveClassification(Status tweet, byte[] img, String classifications) throws IOException {
        Connection conn = getHBaseConnection();
        Table classificationsTable = conn.getTable(TableName.valueOf("tweet_classifications"));
        String rowkey = getTopClassification(classifications) + "#" + tweet.getUser().getScreenName() + "|" + tweet.getId();
        Put put = new Put(Bytes.toBytes(rowkey));
        byte[] cf = Bytes.toBytes("tc");
        put.addColumn(cf,Bytes.toBytes("img"),img);
        put.addColumn(cf,Bytes.toBytes("tId"),Bytes.toBytes(tweet.getId()));
        put.addColumn(cf,Bytes.toBytes("uId"),Bytes.toBytes(tweet.getUser().getId()));
        put.addColumn(cf,Bytes.toBytes("uDn"),Bytes.toBytes(tweet.getUser().getName()));
        put.addColumn(cf,Bytes.toBytes("c"),Bytes.toBytes(classifications));
        classificationsTable.put(put);
        classificationsTable.close();
        conn.close();
    }


    private String getTopClassification(String classifications) {
        return classifications.split("\n")[0].split(" ")[0].trim();
    }

}
