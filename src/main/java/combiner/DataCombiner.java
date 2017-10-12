package combiner;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

import utils.CustomKey;

import java.io.IOException;
import java.util.*;

public class DataCombiner extends Reducer<CustomKey,Text,CustomKey,Text> {

    private final  int DATA_TYPE=2;
    private final  int CITY_TYPE=1;

    @Override
    protected void reduce(CustomKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer count=0;
        Iterator<Text> iterator = values.iterator();
        if(key.getDataSetType()==CITY_TYPE){
            context.write(key,new Text(iterator.next()));
        }
         if(key.getDataSetType()==DATA_TYPE){
            while (iterator.hasNext()){
                count+=Integer.parseInt(iterator.next().toString());
            }
            context.write(key,new Text(count.toString()));
        }

    }

}
