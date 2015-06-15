package di.match;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import di.tools.Vars;

public class StandardOutput {
	public static class StandardOutputMapper extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String s = value.toString();
			if (s.charAt(s.indexOf(" ")+1) == '_'){
				s = s.substring(0, s.indexOf(" ")+1) + s.substring(s.indexOf(" ")+4);
				outKey.set(s);
				outValue.set("");
			}
			else{
				outKey.set(s.substring(0, s.indexOf(" ", s.indexOf(" ")+1)));
				outValue.set(s.substring(s.indexOf(" ",s.indexOf(" ")+1)+1));
			}
			context.write(outKey, outValue);
		};
	}
	
	public static class StandardOutputReducer extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
			String s = "";
			boolean flag = false;
			for (Text value:values){
				String t = value.toString();
				if (t.length() == 0)
					flag = true;
				else s = t;
			}
			if (s.length() > 0)
				if (flag){
					outKey.set(s + ",1");
					context.write(outKey, outValue);
				}
				else 
				{
					outKey.set(s + ",0");
					context.write(outKey, outValue);
				}
		};
	}
	
	public void run(String dataSet1, String dataSet2, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		String dataSet = dataSet1 + "_" + dataSet2;		
		Configuration conf = new Configuration();
		Job job = new Job( conf, "DI:Match#Std_Output_" + dataSet );
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(StandardOutput.class);
		
		FileInputFormat.addInputPath(job, new Path(Vars.DisCalc + dataSet));
		FileInputFormat.addInputPath(job, new Path(Vars.CorrectAlignment + dataSet));
		
		job.setMapperClass( StandardOutputMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( StandardOutputReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.StandardOutput + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
