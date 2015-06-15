package di.evaluate;

import java.io.IOException;

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

public class RewriteDBpediaRefFreebase {
	public static class RewriteDBpediaRefFreebaseMapper extends Mapper<Object, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		String labelProperty;
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String[] q = TripleUtil.parseTriple(value.toString());
			String s = q[2].substring(0, q[2].indexOf("/ns/") + 4) + 
					q[2].substring(q[2].indexOf("/ns/") + 4).replace('/', '.');
			outKey.set(q[0] + " _c_" + s);
			context.write(outKey, outValue);
		};
	}
	
	public void run(int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		Job job = new Job( conf, "DI:Evaluate#Rewrite_DBP_Ref_FB");
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(RewriteDBpediaRefFreebase.class);
		
		FileInputFormat.addInputPath( job, new Path(Vars.Rawdata + "DBpediaRefFreebase") );
		
		job.setMapperClass(RewriteDBpediaRefFreebaseMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setReducerClass( Reducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.CorrectAlignment + Vars.DBP + "_" + Vars.FB;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
