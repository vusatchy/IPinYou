package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.CustomKey;
import utils.IPinYouParser;

import java.io.IOException;

public class CityMapper extends Mapper<LongWritable, Text, CustomKey, Text> {
    Integer intTwo = new Integer(2);
    CustomKey customKey;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String [] cities=value.toString().split("\t");
        if (cities.length==intTwo){
            customKey=new CustomKey(Integer.parseInt(cities[0]),intTwo);
            context.write(customKey,intWritableOne);
        }
    }
    
}
