package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.CustomKey;
import utils.IPinYouParser;

import java.io.IOException;

public class CityMapper extends Mapper<LongWritable, Text, CustomKey, Text> {

    private IntWritable intWritableOne = new IntWritable(1);
    private Integer ONE = new Integer(1);
    private static  final  int TWO=2;

    CustomKey customKey;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String [] cities=value.toString().split("\t");
        if (cities.length==TWO){
            customKey=new CustomKey(Integer.parseInt(cities[0]),ONE);
            context.write(customKey,new Text(cities[1]));
        }
    }

}
