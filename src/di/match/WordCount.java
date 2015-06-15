package di.match;

import java.io.IOException;
import java.util.HashSet;

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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import di.tools.Vars;

public class WordCount {
	static int M = 30;
	public static class WordCountMapper extends Mapper<Object, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		HashSet<String> set = new HashSet<String>();
		
		void collect(JSONArray a) throws JSONException{
			for (int i=0; i<a.length(); ++i)
				set.add((String)a.get(i));
		}
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			try {
				JSONObject json = new JSONObject(value.toString());
				set.clear();
				collect(json.getJSONArray(Vars.label));
				collect(json.getJSONArray(Vars.textLong));
				collect(json.getJSONArray(Vars.textMiddle));
				collect(json.getJSONArray(Vars.textShort));
				collect(json.getJSONArray(Vars.property));
				for (String i:set){
					outKey.set(i);
					context.write(outKey, outValue);
				}
					
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
		};
	}
	
	public static class WordCountReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws java.io.IOException ,InterruptedException {
			int sum = 1;
			for (NullWritable i:values)
				++sum;
			outKey.set(key.toString() + " " + sum);
			context.write(outKey, outValue);
		};
	}
	
	public void run(String[] dataSets, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		String dataSet = Vars.combDataSets(dataSets);
		Job job = new Job( conf, "DI:Match#Word_Count_" + dataSet);
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(WordCount.class);
		
		for (String i:dataSets)
			FileInputFormat.addInputPath( job, new Path(Vars.Json + i) );
		
		job.setMapperClass( WordCountMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setReducerClass( WordCountReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.WordCount + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
