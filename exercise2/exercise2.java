import java.io.*;
import java.util.*;
import java.util.regex.*; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



public class exercise2 extends Configured implements Tool {
	

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, myPair> {
		
        //Declare mapper output variables outside of map function
        //If you were mapping one billion rows, this operation would still be completed once per Map initiation,
        //rather than once per row
    	
		public void configure(JobConf job) {
		}
		
		protected void setup(OutputCollector<Text, myPair> output) throws IOException, InterruptedException {
			//The setup job is ran once in each map task. Here you could initialize a set of words which
			//you want to exclude from word count, like a set of stop words.
			// example: https://stackoverflow.com/questions/25432598/what-is-the-mapper-of-reducer-setup-used-for
		}

		public void map(LongWritable key, Text value, 
			OutputCollector<Text, myPair> output, 
			Reporter reporter) throws IOException {
		    
		    // read and split the line
		    String line = value.toString();
		    String[] splitted = line.split("\\s");

		    // check whether the line comes from the first or the second file
		    if (splitted.length == 4){
		    	// unigram
				String volumes_str = splitted[3].trim();
				double x_i = Double.parseDouble(volumes_str);
				double x_i_square = x_i*x_i;
				double n = 1;
				output.collect(new Text("myKey"), new myPair(x_i_square, n, x_i));
		    }
		    else if (splitted.length == 5){
		    	// bigram
				String volumes_str = splitted[4].trim();
				double x_i = Double.parseDouble(volumes_str);
				double x_i_square = x_i*x_i;
				double n = 1;
				output.collect(new Text("myKey"), new myPair(x_i_square, n, x_i));
		    }
		}
		
		protected void cleanup(OutputCollector<Text, myPair> output) throws IOException, InterruptedException {
			//The cleanup job is ran once in each map task, similar to the setup function
		}
	}
	
    public static class Reduce extends MapReduceBase implements Reducer<Text, myPair, Text, DoubleWritable> {

    	public void reduce(Text key, Iterator<myPair> values, 
    		OutputCollector<Text, DoubleWritable> output, 
    		Reporter reporter) throws IOException {
			double sum_of_square = 0.0, count = 0.0, sum = 0.0;
			while (values.hasNext()) {
				double[] vs = values.next().get();
				sum_of_square += vs[0];
				count += vs[1];
				sum += vs[2];
			}
			double avg = sum/count;
			double sd = Math.sqrt(1/count * (sum_of_square - count*avg*avg));
	        output.collect(key, new DoubleWritable(sd));
		}
    }

    //configurations
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), exercise2.class);
		
		//set output delimiter to comma
		//conf.set("mapred.textoutputformat.separator", ","); 
		conf.setJobName("exercise2");
		
		conf.setMapOutputKeyClass(Text.class);  //map key
	    conf.setMapOutputValueClass(myPair.class);  //map value
	    
	    conf.setOutputKeyClass(Text.class);  //reduce key
	    conf.setOutputValueClass(DoubleWritable.class);  //reduce value
		
		conf.setMapperClass(Map.class);
	    //conf.setCombinerClass(Reduce.class);
	    conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		return 0;
    }
    
    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise2(), args);
		System.exit(res);
    }//main
}


