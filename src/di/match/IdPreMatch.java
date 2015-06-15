package di.match;

import java.io.IOException;
import java.util.ArrayList;
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
import org.json.JSONException;
import org.json.JSONObject;

import di.tools.Vars;

public class IdPreMatch {
	static int M = 10000;
	public static class IdPreMatchMapper extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		long counter;
		Random random = new Random();
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			try {
				String s = value.toString();
				if (s.startsWith("<")){
					if (random.nextInt(1000) < 1000){
						outValue.set("_" + ++counter);
						outKey.set(s.substring(0, s.indexOf(" ")));
						context.write(outKey, outValue);
						String t = s.substring(s.indexOf(" ")+1);
						if (t.startsWith("_c_"))
							t = t.substring(3);
						outKey.set(t);
						context.write(outKey, outValue);
					}
				}
				else{
					JSONObject json = new JSONObject(s);
					outKey.set(json.getString(Vars.uri));
					outValue.set(s);
					context.write(outKey, outValue);
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}			
		};
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			String taskId = context.getConfiguration().get("mapred.task.id")
					.substring(context.getConfiguration().get("mapred.task.id").indexOf("_m_") + 3);
			taskId = taskId.replaceAll("_", "");
			counter = (Long.valueOf(taskId).longValue() + 1) << 32;
		};
	}
	
	public static class IdPreMatchReducer extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		ArrayList<String> list = new ArrayList<String>();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
			list.clear();
			String json = null;
			for (Text value:values){
				String s = value.toString();
				if (s.startsWith("_"))
					if (json == null)
						list.add(s);
					else{
						outKey.set(s+ " " + json);
						context.write(outKey, outValue);
					}
				else {
					json = s;
					for (String i:list){
						outKey.set(i + " " + json);
						context.write(outKey, outValue);
					}
				}
			}
		};
	}
	
	public void run(String dataSet1, String dataSet2, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		String dataSet = dataSet1 + "_" + dataSet2;		
		Configuration conf = new Configuration();
		Job job = new Job( conf, "DI:Match#Id_Pre_Match_" + dataSet );
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(IdPreMatch.class);
		
		FileInputFormat.addInputPath(job, new Path(Vars.PreMatch + dataSet));
		if (dataSet1.equals(Vars.DBP) && dataSet2.equals(Vars.GN))
			FileInputFormat.addInputPath(job, new Path(Vars.CorrectAlignment + dataSet));
		if (dataSet1.equals(Vars.DBP) && dataSet2.equals(Vars.FB))
			FileInputFormat.addInputPath(job, new Path(Vars.CorrectAlignment + dataSet));
		FileInputFormat.addInputPath(job, new Path(Vars.Json + dataSet1));
		FileInputFormat.addInputPath(job, new Path(Vars.Json + dataSet2));
		
		job.setMapperClass( IdPreMatchMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( IdPreMatchReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.IdPreMatch + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
