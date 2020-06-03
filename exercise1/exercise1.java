import java.io.*;
import java.util.*;
import java.util.regex.*; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise1 extends Configured implements Tool {
	

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
                  
		public boolean yearValidation(String year){
			// regular expression pattern to validate year
		    Pattern yearPattern = Pattern.compile("\\d{4}");
		    return yearPattern.matcher(year).matches();
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		    //Read first value (line)
		    String line = value.toString();

		    // split the line to an array of Strings
		    String[] splitted = line.split("\\s");

		    // check whether the line comes from the first or the second file
		    if (splitted.length == 4){
		    	// unigram
		    	String raw_word = splitted[0].trim();
				String year = splitted[1].trim();
				String volumes = splitted[3].trim();
				if (yearValidation(year)){
					if (raw_word.toLowerCase().contains("nu")){
						output.collect(new Text(year+","+"nu"), new Text(volumes+", 1.0"));
					}
					if (raw_word.toLowerCase().contains("chi")){
						output.collect(new Text(year+","+"chi"), new Text(volumes+", 1.0"));

					}
					if (raw_word.toLowerCase().contains("haw")){
						output.collect(new Text(year+","+"haw"), new Text(volumes+", 1.0"));
					}
				}
		    }
		    else if (splitted.length == 5){
		    	// bigram
		    	String raw_word1 = splitted[0].trim();
				String raw_word2 = splitted[1].trim();
				String year = splitted[2].trim();
				String volumes = splitted[4].trim();
				if (yearValidation(year)){
					// for the first word
					if (raw_word1.toLowerCase().contains("nu")){
						output.collect(new Text(year+","+"nu"), new Text(volumes+", 1.0"));
					}
					if (raw_word1.toLowerCase().contains("chi")){
						output.collect(new Text(year+","+"chi"), new Text(volumes+", 1.0"));

					}
					if (raw_word1.toLowerCase().contains("haw")){
						output.collect(new Text(year+","+"haw"), new Text(volumes+", 1.0"));
					}
					// same for the second word
					if (raw_word2.toLowerCase().contains("nu")){
						output.collect(new Text(year+","+"nu"), new Text(volumes+", 1.0"));
					}
					if (raw_word2.toLowerCase().contains("chi")){
						output.collect(new Text(year+","+"chi"), new Text(volumes+", 1.0"));

					}
					if (raw_word2.toLowerCase().contains("haw")){
						output.collect(new Text(year+","+"haw"), new Text(volumes+", 1.0"));
					}
				}
		    }
		}
		
		protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
			//The cleanup job is ran once in each map task, similar to the setup function
		}
	}

    public static class AvgCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            double sum = 0.0, count = 0.0;
	        while (values.hasNext()) {
				Text pair = values.next();
				String[] tokens = pair.toString().split(",");
				count++;  // update the counter
		    	sum += Double.parseDouble(tokens[0]);  // sum up the volumes
		    }
	        output.collect(key, new Text(Double.toString(sum)+", "+Double.toString(count)));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {

	    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            double sum = 0.0, count = 0.0, avg = 0.0;
	        while (values.hasNext()) {
				Text pair = values.next();
				String[] tokens = pair.toString().split(",");
		        sum += Double.parseDouble(tokens[0]);             // sum up the volumes
		        count = count + Double.parseDouble(tokens[1]);    // update the counter
		    }
			avg = sum/count;
	        output.collect(key, new DoubleWritable(avg));
        }
    }

    //configurations
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), exercise1.class);
		
		//set output delimiter to comma
		conf.set("mapred.textoutputformat.separator", ","); 
		conf.setJobName("exercise1");
		
		conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Text.class);
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setMapperClass(Map.class);
	    conf.setCombinerClass(AvgCombiner.class);
	    conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		return 0;
    }
    
    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise1(), args);
		System.exit(res);
    }
}
