package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.CustomKey;
import utils.IPinYouParser;

import java.io.IOException;
import java.util.Arrays;




public class DataMapper extends Mapper<LongWritable, Text, CustomKey, Text> {

    private  final Integer intTwo = new Integer(2);
    CustomKey customKey ;
    private final Integer ONE = new Integer(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        IPinYouParser parser = new IPinYouParser(value.toString());
        customKey=new CustomKey(parser.getCityID(),intTwo);
        context.write(customKey,new Text(ONE.toString()));
    }
}
