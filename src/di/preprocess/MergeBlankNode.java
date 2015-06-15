package di.preprocess;

import java.io.IOException;
import java.util.ArrayList;

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
import di.tools.TripleUtil;

public class MergeBlankNode {
	public static class MergeBlankNodeMapper extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String q[] = TripleUtil.parseTriple(value.toString());
			if (q[2].startsWith("_:")){
				outKey.set(q[2]);
				outValue.set("M"+q[0] + " " + q[1]);
			}
			else{
				outKey.set(q[0]);
				outValue.set(q[1] + " " + q[2]);
			}
			context.write(outKey, outValue);
		};
	}
	
	public static class MergeBlankNodeReducer extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
			ArrayList<String> list = new ArrayList<String>();
			String main = "";
			if (key.toString().startsWith("_:")){
				for (Text value : values){
					if (value.toString().startsWith("M")){
						main = value.toString().substring(1);
						for (String s:list){
							outKey.set(main.substring(0, main.length()-1) + ',' + s.substring(1) + " .");
							context.write(outKey, outValue);
						}
					}
					else if (main.length() == 0)
						list.add(value.toString());
					else {
						outKey.set(main.substring(0, main.length()-1) + ',' + value.toString().substring(1) + " .");
						context.write(outKey, outValue);						
					}
				}
			}
			else
				for (Text value : values){
					outKey.set(key.toString() + " " + value.toString() + " .");
					context.write(outKey, outValue);
				}
		};
	}
	
	public void run(String dataSet, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		Job job = new Job( conf, "DI:Preprocessing#Merge_BN_" + dataSet );
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(MergeBlankNode.class);
		
		FileInputFormat.addInputPath( job, new Path(Vars.PropertyText + dataSet) );
		
		job.setMapperClass( MergeBlankNodeMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( MergeBlankNodeReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.MergedBN + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
