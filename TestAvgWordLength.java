package stubs;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestAvgWordLength {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		LetterMapper mapper = new LetterMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		AverageReducer reducer = new AverageReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper() {

		mapDriver.withInput(new LongWritable(1), new Text("No now is definitely not the best time"));
		mapDriver.withOutput(new Text("N"), new IntWritable(2));
		mapDriver.withOutput(new Text("n"), new IntWritable(3));
		mapDriver.withOutput(new Text("i"), new IntWritable(2));
		mapDriver.withOutput(new Text("d"), new IntWritable(10));
		mapDriver.withOutput(new Text("n"), new IntWritable(3));
		mapDriver.withOutput(new Text("t"), new IntWritable(3));
		mapDriver.withOutput(new Text("b"), new IntWritable(4));
		mapDriver.withOutput(new Text("t"), new IntWritable(4));
		mapDriver.runTest();

	}

    /*
     * Test the reducer.
     */
    @Test
    public void testReducerN(){
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(2));
        reduceDriver.withInput(new Text("N"), values);
        reduceDriver.withOutput(new Text("N"), new DoubleWritable(2.0));
        reduceDriver.runTest();
    }
    
    @Test
    public void testReducerb(){
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(4));
        reduceDriver.withInput(new Text("b"), values);
        reduceDriver.withOutput(new Text("b"), new DoubleWritable(4.0));
        reduceDriver.runTest();
    }
    
    @Test
    public void testReducerd(){
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(10));
        reduceDriver.withInput(new Text("d"), values);
        reduceDriver.withOutput(new Text("d"), new DoubleWritable(10.0));
        reduceDriver.runTest();
    }
    
    @Test
    public void testReduceri(){
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(2));
        reduceDriver.withInput(new Text("i"), values);
        reduceDriver.withOutput(new Text("i"), new DoubleWritable(2.0));
        reduceDriver.runTest();
    }
    
    @Test
    public void testReducern(){
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(3));
        values.add(new IntWritable(3));
        reduceDriver.withInput(new Text("n"), values);
        reduceDriver.withOutput(new Text("n"), new DoubleWritable(3.0));
    }
    
    @Test
    public void testReducet(){
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(3));
        values.add(new IntWritable(4));
        reduceDriver.withInput(new Text("t"), values);
        reduceDriver.withOutput(new Text("t"), new DoubleWritable(3.5));
    }

    /*
     * Test the Driver 
     */
    @Test
    public void testMapReduce() {
    	mapReduceDriver.withInput(new LongWritable(1), new Text("No now is definitely not the best time")); 
    	mapReduceDriver.addOutput(new Text("N"), new DoubleWritable(2.0));
    	mapReduceDriver.addOutput(new Text("b"), new DoubleWritable(4.0));
    	mapReduceDriver.addOutput(new Text("d"), new DoubleWritable(10.0));
    	mapReduceDriver.addOutput(new Text("i"), new DoubleWritable(2.0));
    	mapReduceDriver.addOutput(new Text("n"), new DoubleWritable(3.0));
    	mapReduceDriver.addOutput(new Text("t"), new DoubleWritable(3.5));
    }

}
