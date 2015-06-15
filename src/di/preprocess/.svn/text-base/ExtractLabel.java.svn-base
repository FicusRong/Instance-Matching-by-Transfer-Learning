package di.preprocess;

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

public class ExtractLabel {
	public static String labelProperty(String dataSet){
		if (dataSet.equals(Vars.FB))
			return "<http://rdf.freebase.com/ns/type.object.name>";
		if (dataSet.equals(Vars.GN))
			return "<http://www.geonames.org/ontology#name>";
		return "<http://www.w3.org/2000/01/rdf-schema#label>";
	}
	
	public static class ExtractLabelMapper extends Mapper<Object, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		String labelProperty;
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			try{
				String[] q = TripleUtil.parseTriple(value.toString());
				if (q[1].equals(labelProperty)){
					outKey.set(q[0] + " " + Vars.labelProperty + " " + q[2] + " .");
					context.write(outKey, outValue);
				}
				else context.write(value, outValue);
			}
			catch (Exception e) {
				System.out.println(value.toString());
				e.printStackTrace();
			}
		};
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			labelProperty = labelProperty(context.getConfiguration().get(Vars.dataSet));
		};
	}
	
	public void run(String dataSet, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set(Vars.dataSet, dataSet);
		Job job = new Job( conf, "DI:Preprocessing#Ext_Label_" + dataSet );
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(ExtractLabel.class);
		
		FileInputFormat.addInputPath( job, new Path(Vars.Rawdata + dataSet) );
		
		job.setMapperClass( ExtractLabelMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setReducerClass( Reducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.ExtraLabel + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
