package reducer;

import org.apache.commons.io.input.TeeInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.CustomKey;

import java.io.IOException;
import java.util.Iterator;

public class IPinYouReducer extends Reducer<CustomKey,Text,Text,IntWritable> {

    private final  int DATA_TYPE=2;
    private final  int CITY_TYPE=1;

    protected void reduce(CustomKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count=0;
        Text name=new Text("test");
        Iterator<Text> iterator = values.iterator();
        name = new Text(iterator.next());
        while (iterator.hasNext()) {
            count += Integer.parseInt(iterator.next().toString());
        }

        if(count!=0) context.write(name,new IntWritable(count));
    }
}
