package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.apache.storm.starter.spout.InputSentence;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.Random;
import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;


public class GetSentenceFromFileSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(GetSentenceFromFileSpout.class);

    SpoutOutputCollector _collector;
    File _input_file;
    Scanner _reader;
    //String[] sentences;
    int msgId;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

      
      _collector = collector;
      // sentences = InputSentence.text.split("\\.") 
      // msgId = 0;

      //String file_path = System.getProperty("user.dir") + "/resources/2mb";
      try{

        // List<String> re_list = getResourceFiles("/");
        // for (String name : re_list){
        //   System.out.println(name);
        // }
        //String file_name = GetSentenceFromFileSpout.class.getResource("resources/2mb").getFile();
        _input_file = new File("resources/2mb");
        _reader = new Scanner(_input_file);
        msgId = 0;
      }catch (FileNotFoundException | NullPointerException e){

        LOG.error("open file error!", e);
        LOG.error(System.getProperty("user.dir"));
        
      }
      
    }
  
    @Override
    public void nextTuple() {
      //Utils.sleep(100);
      String[] sentences = new String[]{sentence("the cow jumped over the moon"), sentence("an apple a day keeps the doctor away"),
             sentence("four score and seven years ago"), sentence("snow white and the seven dwarfs"), sentence("i am at two with nature")};
      if(_reader.hasNextLine()){
         final String sentence = _reader.nextLine();
         LOG.debug("Emitting tuple: {}", sentence);
         _collector.emit(new Values(sentence), msgId);
         msgId++;
      }else {
          return;
      }

      // if(msgId)
      //   _collector.emit(new Values(sentences[msgId]), msgId);
      //   msgId++;
      // }
  
     
    }

//   private List<String> getResourceFiles(String path) throws IOException {
//       List<String> filenames = new ArrayList<>();
  
//       try (
//               InputStream in = getResourceAsStream(path);
//               BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
//           String resource;
  
//           while ((resource = br.readLine()) != null) {
//               filenames.add(resource);
//           }
//       }
  
//       return filenames;
//   }
  

    protected String sentence(String input) {
      return input;
    }
  
    @Override
    public void ack(Object id) {
    }
  
    @Override
    public void fail(Object id) {
    }
  
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  
    // Add unique identifier to each tuple, which is helpful for debugging
//     public static class TimeStamped extends RandomSentenceSpout {
//       private final String prefix;
  
//       public TimeStamped() {
//         this("");
//       }
  
//       public TimeStamped(String prefix) {
//         this.prefix = prefix;
//       }
  
//       protected String sentence(String input) {
//         return prefix + currentDate() + " " + input;
//       }
  
//       private String currentDate() {
//         return new SimpleDateFormat("yyyy.MM.dd_HH:mm:ss.SSSSSSSSS").format(new Date());
//       }
//     }
// 
}
