package di.evaluate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import di.tools.Vars;

public class CalcRecall {
	public static class CalcRecallMapper extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String s = value.toString();
			outKey.set(s.substring(0, s.indexOf(' ')));
			outValue.set(s.substring(s.indexOf(' ')+1));
			context.write(outKey, outValue);
		};
	}
	
	public static class CalcRecallReducer extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		ArrayList<String> list = new ArrayList<String>();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			list.clear();
			for (Text value:values){
				String s = value.toString();
				if (s.startsWith("_c_"))
					map.put(s.substring(3), 0);
				else list.add(s);
			}
			for (String i:list)
				if (map.containsKey(i))
					map.put(i, 1);
			for (String i:map.keySet()){
				if (map.get(i) == 1)
					outKey.set("1");
				else{
					outKey.set("0");
					System.out.println(key.toString() + " " + i);
				}
				context.write(outKey, outValue);
			}
		};
	}
	
	public void run(String dataSet1, String dataSet2, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		String dataSet = dataSet1 + "_" + dataSet2;
		Configuration conf = new Configuration();
		Job job = new Job( conf, "DI:Evaluate#Calc_Recall_" + dataSet);
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(CalcRecall.class);
		
		FileInputFormat.addInputPath( job, new Path(Vars.CorrectAlignment + dataSet) );
		FileInputFormat.addInputPath( job, new Path(Vars.PreMatch + dataSet) );
		
		job.setMapperClass(CalcRecallMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( CalcRecallReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.TestOutput;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
		
		TextInputFormat reader = new TextInputFormat();
		Job tempJob = new Job(new Configuration());
		FileInputFormat.addInputPath(tempJob, new Path(outputPath));
		List<InputSplit> inputSplits = reader.getSplits(tempJob);
		TaskAttemptID tempID = new TaskAttemptID(new TaskID("tempID", 0, true, 1), 0);
		TaskAttemptContext tempContext = new TaskAttemptContext(tempJob.getConfiguration(), tempID);
		double total = 0;
		double recall = 0;
		for (InputSplit split : inputSplits) {
			LineRecordReader recordReader = (LineRecordReader) reader.createRecordReader(split, tempContext);
			recordReader.initialize(split, tempContext);
			while(recordReader.nextKeyValue()){
				String s = recordReader.getCurrentValue().toString();
				total += 1;
				if (s.equals("1"))
					recall += 1;
			}
		}
		System.out.println("Recall: " + recall/total);
		System.out.println(recall + " " + total);
	}
}
