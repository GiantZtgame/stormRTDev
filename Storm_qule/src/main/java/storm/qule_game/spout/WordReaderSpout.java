package storm.qule_game.spout;

import java.io.*;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

public class WordReaderSpout extends BaseRichSpout {
    BufferedReader _sampleLogReader;
    SpoutOutputCollector _collector;
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext context;

    public void ack(Object msgId) {System.out.println("OK:"+msgId);}
    public void close() {}
    public void fail(Object msgId) {System.out.println("FAIL:"+msgId);}

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//        try {
//            this.context = context;
//            this.fileReader = new FileReader(conf.get("wordsFile").toString());
//        } catch (FileNotFoundException e) {
//            throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
//        }
        InputStream in = getClass().getResourceAsStream("/samplebilllog.txt");
        _sampleLogReader = new BufferedReader(new InputStreamReader(in));
        _collector = collector;
    }
    /**
     *  发射一个Tuple到Topology
     */
//    public void nextTuple() {
//        if (completed) {
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {}
//            return;
//        }
//        String str;
//        BufferedReader reader = new BufferedReader(fileReader);
//        try {
//            while ((str = reader.readLine()) != null) {
//                this.collector.emit(new Values(str), str);
//            }
//        } catch (Exception e) {
//                throw new RuntimeException("Error reading tuple",e);
//        } finally {
//                completed = true;
//        }
//    }
    @Override
    public void nextTuple() {
        Utils.sleep(1000);

        String sampleLogLine = null;
        try{
            sampleLogLine = _sampleLogReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String sentence = sampleLogLine.toString();
        _collector.emit(new Values(sentence));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("line"));}
}