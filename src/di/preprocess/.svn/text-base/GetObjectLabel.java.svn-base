package di.preprocess;

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

import di.tools.Vars;
import di.tools.TripleUtil;

public class GetObjectLabel {
	static int M = 30;
	public static class GetObjectLabelMapper extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		String s;
		Random random = new Random();
		String dataSet;
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String[] q = TripleUtil.parseTriple(value.toString());
			if (q[2].startsWith("<")){
				outKey.set("L" + random.nextInt()%M + q[2]);
				outValue.set(q[0] + " " + q[1]);
				context.write(outKey, outValue);
			}
			else{
				s = TripleUtil.neededLiteral(q[2]);
				if (s != null){
					outKey.set(q[0]+" "+q[1]+" "+s+" .");
					outValue.set("");
					context.write(outKey, outValue);
					if (q[1].equals(Vars.labelProperty)){
						for (int i=0; i<M; ++i){
							outKey.set("L" + i + q[0]);
							outValue.set(s);
							context.write(outKey, outValue);
						}
					}
				}
			}
		};
	}
	
	public static class GetObjectLabelReducer extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		ArrayList<String> list = new ArrayList<String>();
		boolean flag;
		String s, t;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
			if (key.toString().startsWith("L")){
				list.clear();
				flag = false;
				for (Text value : values){
					s = value.toString();
					if (s.charAt(0) == '"'){
						flag = true;
						t = s;
						for (String i : list){
							outKey.set(i + " " + t + " .");
							context.write(outKey, outValue);
						}
					}
					else
						if (flag){
							outKey.set(s + " " + t + " .");
							context.write(outKey, outValue);
						}
						else list.add(s);
				}
				if (!flag){
					t = key.toString();
					t = t.substring(t.indexOf('<'));					
					for (String i : list){
						outKey.set(i + " " + t + " .");
						context.write(outKey, outValue);						
					}						
				}
			}
			else{
				outKey.set(key);
				context.write(outKey, outValue);
			}
		};
	}
	
	public void run(String dataSet, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		Job job = new Job( conf, "DI:Preprocessing#Get_O_Label_" + dataSet );
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(GetObjectLabel.class);
		
		FileInputFormat.addInputPath( job, new Path(Vars.ExtraLabel + dataSet) );
		
		job.setMapperClass( GetObjectLabelMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( GetObjectLabelReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.LiteralObject + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
