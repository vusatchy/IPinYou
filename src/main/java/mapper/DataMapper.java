package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.CustomKey;
import utils.IPinYouParser;

import java.io.IOException;


public class DataMapper extends Mapper<LongWritable, Text, CustomKey, IntWritable> {

    IntWritable intWritableOne = new IntWritable(1);
    Integer intOne = new Integer(1);
    CustomKey customKey ;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        IPinYouParser parser = new IPinYouParser(value.toString());
        if (parser.isValidForMapping()){
            customKey=new CustomKey(parser.getCityID(),intOne);
            context.write(customKey,intWritableOne);
        }
    }
}
