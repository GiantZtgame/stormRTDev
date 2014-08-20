package storm.qule_game.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by WXF on 2014/7/15.
 */
public class SampleLogSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    InputStream log_input;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;

//        try {
//            byte[] buffer = new byte[1024];
//            log_input = getClass().getResourceAsStream("/samplelog.txt");
//
//            System.out.println("---------------" + log_input);
//
//            int bytesRead = 0;
//            while ((bytesRead = log_input.read(buffer)) != -1) {
//
//            }
//        } catch (FileNotFoundException ex) {
//            ex.printStackTrace();
//        } catch (IOException ex) {
//            ex.printStackTrace();
//        } finally {
//            //关闭 BufferedInputStream
//            try {
//                InputStream bufferedInput;
//                if (bufferedInput != null)
//                    bufferedInput.close();
//            } catch (IOException ex) {
//                ex.printStackTrace();
//            }
//        }
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);

        String sentence = "This is temp text.";
        _collector.emit(new Values(sentence));
    }

    @Override
    public void ack(Object id) {

    }

    @Override
    public void fail(Object id) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("samplelog"));
    }
}
