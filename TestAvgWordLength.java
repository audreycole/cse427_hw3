package stubs;

import static org.junit.Assert.*;

import java.io.IOException;
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
 * 		 * Set up the mapper test harness.
 * 		 		 */
		LetterMapper mapper = new LetterMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

		/*
 * 		 * Set up the reducer test harness.
 * 		 		 */
		AverageReducer reducer = new AverageReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		/*
 * 		 * Set up the mapper/reducer test harness.
 * 		 		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	/*
 * 	 * Test the mapper.
 * 	 	 */
	@Test
	public void testMapper() throws IOException {

		mapDriver.withInput(new LongWritable(1), new Text("No now is definitely not the best time"));
		mapDriver.withOutput(new Text("N"), new IntWritable(2));
		mapDriver.withOutput(new Text("n"), new IntWritable(3));
		mapDriver.withOutput(new Text("i"), new IntWritable(2));
		mapDriver.withOutput(new Text("d"), new IntWritable(10));
		mapDriver.withOutput(new Text("n"), new IntWritable(3));
		mapDriver.withOutput(new Text("t"), new IntWritable(3));
		mapDriver.withOutput(new Text("b"), new IntWritable(4));
		mapDriver.withOutput(new Text("t"), new IntWritable(4));
		System.out.println("Mapper Result: ");
		System.out.println(mapDriver.run());
		mapDriver.runTest();

	}

    /*
 *      * Test the reducer.
 *           */
    @Test
    public void testReducerN() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(2));
        reduceDriver.withInput(new Text("N"), values);
        reduceDriver.withOutput(new Text("N"), new DoubleWritable(2.0));
        System.out.println("Reducer N Result: ");
        System.out.println(reduceDriver.run());
        reduceDriver.runTest();
    }
    
    @Test
    public void testReducerb() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(4));
        reduceDriver.withInput(new Text("b"), values);
        reduceDriver.withOutput(new Text("b"), new DoubleWritable(4.0));
        System.out.println("Reducer b Result: ");
        System.out.println(reduceDriver.run());
        reduceDriver.runTest();
    }
    
    @Test
    public void testReducerd() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(10));
        reduceDriver.withInput(new Text("d"), values);
        reduceDriver.withOutput(new Text("d"), new DoubleWritable(10.0));
        System.out.println("Reducer d Result: ");
        System.out.println(reduceDriver.run());
        reduceDriver.runTest();
    }
    
    @Test
    public void testReduceri() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(2));
        reduceDriver.withInput(new Text("i"), values);
        reduceDriver.withOutput(new Text("i"), new DoubleWritable(2.0));
        System.out.println("Reducer i Result: ");
        System.out.println(reduceDriver.run());
        reduceDriver.runTest();
    }
    
    @Test
    public void testReducern() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(3));
        values.add(new IntWritable(3));
        reduceDriver.withInput(new Text("n"), values);
        reduceDriver.withOutput(new Text("n"), new DoubleWritable(3.0));
        System.out.println("Reducer n Result: ");
        System.out.println(reduceDriver.run());
        reduceDriver.runTest();
    }
    
    @Test
    public void testReducet() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(3));
        values.add(new IntWritable(4));
        reduceDriver.withInput(new Text("t"), values);
        reduceDriver.withOutput(new Text("t"), new DoubleWritable(3.5));
        System.out.println("Reducer t Result: ");
        System.out.println(reduceDriver.run());
        reduceDriver.runTest();
    }

    /*
 *      * Test the Driver 
 *           */
    @Test
    public void testMapReduce() throws IOException {
    	mapReduceDriver.withInput(new LongWritable(1), new Text("No now is definitely not the best time")); 
    	mapReduceDriver.addOutput(new Text("N"), new DoubleWritable(2.0));
    	mapReduceDriver.addOutput(new Text("b"), new DoubleWritable(4.0));
    	mapReduceDriver.addOutput(new Text("d"), new DoubleWritable(10.0));
    	mapReduceDriver.addOutput(new Text("i"), new DoubleWritable(2.0));
    	mapReduceDriver.addOutput(new Text("n"), new DoubleWritable(3.0));
    	mapReduceDriver.addOutput(new Text("t"), new DoubleWritable(3.5));
    	System.out.println("MapReduce Result: ");
    	System.out.println(mapReduceDriver.run());
    	System.out.println();
    	mapReduceDriver.runTest();
    	
    }

}

