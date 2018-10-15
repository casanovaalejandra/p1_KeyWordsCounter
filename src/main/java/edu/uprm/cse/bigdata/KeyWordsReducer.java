package edu.uprm.cse.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class KeyWordsReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        // key is the text of the tweet
        // values is a list of 1s, one for each time the word was found

        // setup a counter
        int count = 0;
        // iterator over list of 1s, to count them (no size() or length() method available)
        for (IntWritable value : values ){
            //System.out.println(key+" " + value);
            count ++;
        }
        // emit key-pair: key, count
        // key is the word
        // count is the number of times that word appeared on the tweets
        Logger logger = LogManager.getRootLogger();
        //logger.trace("Red: " + key.toString());

        // DEBUG
        context.write(key, new IntWritable(count));
    }
}
