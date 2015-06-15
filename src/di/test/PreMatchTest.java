package di.test;

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

import di.tools.TripleUtil;
import di.tools.Vars;

public class PreMatchTest {
	public static class PreMatchTestMapper extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String q[] = TripleUtil.parseTriple(value.toString());
			if (q[2].startsWith("<"))
				return;
			String s = TripleUtil.textLiteral(q[2].toLowerCase());
			outKey.set(s);
			outValue.set(q[0]);
			context.write(outKey, outValue);
		};
	}
	
	public static class PreMatchTestReducer extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		String dataSet1, dataSet2;
		HashSet<String> set1 = new HashSet<String>();
		HashSet<String> set2 = new HashSet<String>();
		int total = 0;
		int maxTotal = 30000000;
		int M = 0;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
			if (total > maxTotal)
				return;
			set1.clear();
			set2.clear();
			for (Text value:values){
				String s = value.toString();
				if (s.startsWith(Vars.uriPrefix(dataSet1))){
					set1.add(s);
					if (set1.size() > M)
						return;
				}
				else{
					set2.add(s);
					if (set2.size() > M)
						return;
				}
			}
			if (set1.size()*set2.size() < M*50)
				for (String i:set1)
					for (String j:set2){
						++total;
						outKey.set(i+" "+j);
						context.write(outKey, outValue);
					}					
		};
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			Configuration conf = context.getConfiguration();
			dataSet1 = conf.get(Vars.dataSet + 1);
			dataSet2 = conf.get(Vars.dataSet + 2);
			M = Vars.preMatchM(dataSet1 + "_" + dataSet2);
		};
	}
	
	public void run(String dataSet1, String dataSet2, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		String dataSet = dataSet1 + "_" + dataSet2;		
		Configuration conf = new Configuration();
		conf.set(Vars.dataSet+1, dataSet1);
		conf.set(Vars.dataSet+2, dataSet2);
		Job job = new Job( conf, "DI:Match#Pre_Match_Test_" + dataSet );
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(PreMatchTest.class);
		
		FileInputFormat.addInputPath( job, new Path(Vars.Rawdata + dataSet1) );
		FileInputFormat.addInputPath( job, new Path(Vars.Rawdata + dataSet2) );
		
		job.setMapperClass( PreMatchTestMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( PreMatchTestReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.PreMatch + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
