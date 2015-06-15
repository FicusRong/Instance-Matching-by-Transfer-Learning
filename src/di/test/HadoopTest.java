package di.test;

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
import org.json.JSONException;
import org.json.JSONObject;

import di.tools.Vars;

public class HadoopTest {
	static public class TestMapper extends Mapper<Object, Text, Text, NullWritable>{
		NullWritable outValue = NullWritable.get();
		int sum = 0;
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {
//			try {
//				JSONObject json = new JSONObject(value.toString());
//				if (json.getString(Vars.uri).equals("<http://linkedgeodata.org/triplify/node314928584>"))
//					context.write(value, outValue);
//			} catch (JSONException e) {
//				e.printStackTrace();
//			}
			if (value.toString().toLowerCase().contains("pembrey"))
				context.write(value, outValue);
		};
	}
	
	static public class TestReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		int ma = 0;
		@Override
		protected void reduce(Text arg0, Iterable<NullWritable> arg1, Context arg2) throws IOException ,InterruptedException {
			arg2.write(arg0, NullWritable.get());
		};
	}
	
	public void run() throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		Job job = new Job( conf, "DI#Test" );
		
		job.setNumReduceTasks( 1 );
		job.setJarByClass( HadoopTest.class );
		
		FileInputFormat.addInputPath( job, new Path(Vars.Rawdata + Vars.LGD) );
		FileInputFormat.addInputPath( job, new Path(Vars.Rawdata + Vars.DBP) );
		
		job.setMapperClass( TestMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setReducerClass( TestReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.TestOutput;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
