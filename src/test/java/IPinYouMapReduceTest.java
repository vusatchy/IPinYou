import mapper.CityMapper;
import mapper.DataMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import utils.CustomKey;

import java.io.IOException;

public class IPinYouMapReduceTest {

    MapDriver<LongWritable, Text, CustomKey, Text> mapDriver;

    @Before
    public void setUp() {
        CityMapper mapper = new CityMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }


    @Test
    public void testCityMapper() throws IOException {
        final String testLine = "15\tshanxi";
        mapDriver.withInput(new LongWritable(0), new Text(
                testLine));
        mapDriver.withOutput(new CustomKey(15,1),new Text("shanxi"));
        mapDriver.runTest();
    }

    @Test
    public void testDataMapper() throws IOException {
        final String testLine = "15\tshanxi";
        mapDriver.withInput(new LongWritable(0), new Text(
                testLine));
        mapDriver.withOutput(new CustomKey(15,1),new Text("shanxi"));
        mapDriver.runTest();
    }
}
