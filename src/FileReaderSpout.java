
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  private BufferedReader reader;

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

    String inputFilePath = (String)conf.get("input-file");
    try{
        this.reader = new BufferedReader(new FileReader(inputFilePath));
    }catch(Exception exception){
        exception.printStackTrace();
    }
    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

    try{
      String line = reader.readLine();
      if (line != null)
      {
          _collector.emit(new Values(line));
      }
      else
      {
          Utils.sleep(100);
      }
    }catch(Exception exception){
        exception.printStackTrace();
    }

  }
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
    try{
        reader.close();
    }catch(Exception exception){
        exception.printStackTrace();
    }

  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
