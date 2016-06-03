package com.lukevinton.storm.imageClassifier;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.DisconnectReason;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.xfer.FileSystemFile;
import twitter4j.Status;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.util.Map;

public class ClassifyBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input)  {
        Status tweet = (Status) input.getValueByField("tweet");
        String imgLocation = (String) input.getValueByField("imgFileLocation");
        String fileName = (String) input.getValueByField("imgFileName");
        String remoteFileName = System.getProperty("remoteTemp") + fileName;
        SSHClient ssh = new SSHClient();
        String classifications = "";

        try {
            ssh.connect(System.getProperty("tfsvr"));
        } catch (TransportException e) {
            if (e.getDisconnectReason() == DisconnectReason.HOST_KEY_NOT_VERIFIABLE) {
                String msg = e.getMessage();
                String[] split = msg.split("`");
                String vc = split[3];
                ssh = new SSHClient();
                ssh.addHostKeyVerifier(vc);
                try {
                    ssh.connect(System.getProperty("tfsvr"));
                }
                catch (IOException e2) {
                    //todo: buried
                }
            } else {
                //todo: buried
            }
        }
        catch (IOException e) {
            //todo: buried
        }
        try {
            ssh.authPassword(System.getProperty("tfuser"),System.getProperty("tfpassword"));
            ssh.newSCPFileTransfer().upload(new FileSystemFile(imgLocation), System.getProperty("remoteTemp"));
            final Session session = ssh.startSession();
            String command = "python /tensorflow/tensorflow/models/image/imagenet/classify_image.py --image " + remoteFileName;
            final Session.Command cmd = session.exec(command);
            classifications = IOUtils.readFully(cmd.getInputStream()).toString();
            session.close();
            ssh.close();

        }
        catch (IOException e) {
            //todo: buried
        }


        collector.emit(new Values(tweet, input.getValueByField("img"), classifications));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "img", "classifications"));
    }

    private void SaveImage(String imageUrl, String destinationFile) throws IOException
    {
        URL url = new URL(imageUrl);
        InputStream is = url.openStream();
        OutputStream os = new FileOutputStream(destinationFile);

        byte[] b = new byte[2048];
        int length;

        while ((length = is.read(b)) != -1) {
            os.write(b, 0, length);
        }

        is.close();
        os.close();
    }
}
