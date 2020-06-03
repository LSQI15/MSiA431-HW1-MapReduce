import java.io.*;
import java.util.*;
import java.util.regex.*; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise4 extends Configured implements Tool {
	

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
        //Declare mapper output variables outside of map function
        //If you were mapping one billion rows, this operation would still be completed once per Map initiation,
        //rather than once per row
    	
		public void configure(JobConf job) {
		}
		
		protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
			//The setup job is ran once in each map task. Here you could initialize a set of words which
			//you want to exclude from word count, like a set of stop words.
			// example: https://stackoverflow.com/questions/25432598/what-is-the-mapper-of-reducer-setup-used-for
		}
                  
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		    //Read first value (line)
		    String line = value.toString();

		    // split the line to an array of Strings
		    String[] splitted = line.split(",");

		    // extract only necessary columns 
		   	String artist = splitted[2].trim();
		   	String duration = splitted[3].trim();

		   	output.collect(new Text(artist), new Text(duration));
		}
		
		protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
			//The cleanup job is ran once in each map task, similar to the setup function
		}
	}

	public static class myPartitioner implements Partitioner<Text, Text> {
	    public void configure(JobConf job) {}

		// sorted by the first character of an artist’s name.
	    public int getPartition(Text key, Text value, int numReduceTasks) {
	        if(numReduceTasks == 0) {
	            return 0;
	        }

	        // case-insensitive sorting
	        String frist_char = key.toString().substring(0, 1).toUpperCase();
	        
	        if(frist_char.compareTo("F") < 0){
	            return 0;	//Reducer0: ABCDE  
	        } 
	        else if (frist_char.compareTo("K") < 0){
	            return 1;	//Reducer1: FGHIJ
	        } 
	        else if (frist_char.compareTo("P") < 0){
	            return 2;	//Reducer2: KLMNO 
	        }
	        else if (frist_char.compareTo("U") < 0){
	            return 3;	//Reducer3: PQRST
	        }
	        else { 
	            return 4;	//Reducer4: UVWXYZ
	        }
	    }

	    protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {}
	}	

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {

    	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			double max_duration = 0 ;
			while (values.hasNext()) {
				// update max duration if necessary
				double current_duration = Double.parseDouble(values.next().toString());
				if (current_duration > max_duration){
					max_duration = current_duration;
				}
			}
	        output.collect(key, new DoubleWritable(max_duration));
		}
    }

    //configurations
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), exercise4.class);

		//set output delimiter to comma
		conf.set("mapred.textoutputformat.separator", ","); 
		
		conf.setJobName("exercise4");
		
		conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Text.class);
	    
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
    	
    	conf.setNumReduceTasks(5);
		conf.setPartitionerClass(myPartitioner.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		return 0;
    }
    
    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise4(), args);
		System.exit(res);
    }//main
}
