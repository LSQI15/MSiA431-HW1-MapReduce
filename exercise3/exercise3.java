import java.io.*;
import java.util.*;
import java.util.regex.*; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise3 extends Configured implements Tool {
	

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
		    String[] splitted = line.split(",");

		    // extract only necessary columns 
		    String title = splitted[0].trim();
		   	String album = splitted[1].trim();
		   	String artist = splitted[2].trim();
		   	String duration = splitted[3].trim();
		   	String year = splitted[165].trim();

		   	if (yearValidation(year)){
		   		int year_int = Integer.parseInt(year);
		   		if (year_int >=2000 && year_int <=2010){
		   			// keep only songs published between the years 2000 and 2010
		   			output.collect(new Text(title+","+artist+","+duration), new Text(""));
		   		}
		   	}
		}
		
		protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
			//The cleanup job is ran once in each map task, similar to the setup function
		}
	}

    //configurations
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), exercise3.class);
		
		conf.setJobName("exercise3");
		
		conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Text.class);
	    
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
    	conf.setNumReduceTasks(0);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		return 0;
    }
    
    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise3(), args);
		System.exit(res);
    }//main
}
