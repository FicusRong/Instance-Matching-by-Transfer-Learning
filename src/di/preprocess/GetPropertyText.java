package di.preprocess;

import java.io.IOException;
import java.util.Map;

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

import di.tools.WordTools;
import di.tools.Vars;
import di.tools.TripleUtil;

public class GetPropertyText {
	static public class GetPropertyNameMapper extends Mapper<Object, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		Map<String, Integer> wordMap;
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {
			String q[] = TripleUtil.parseTriple(value.toString());
			if (q[1].equals(Vars.labelProperty)){
				context.write(value, outValue);
				return;
			}
			
			String p = "";
			String s = "";
			int last = q[1].length()-1;
			while(last>0 && !Character.isLetter(q[1].charAt(last)))
				--last;
			for (int i=last; i>=0; --i)
				if (q[1].charAt(i) == '/'  || q[1].charAt(i) == '#' || q[1].charAt(i) == '.'){
					s = q[1].substring(i+1, q[1].length()-1);
					break;
				}
			s = s.toLowerCase();
			String t = "";
			String r = "";
			for (int i=0; i<=s.length(); ++i)
				if (i == s.length() || !Character.isLowerCase(s.charAt(i))){
					while(t.length()>0){
						int j = t.length();
						while(j>0 && !wordMap.containsKey(t.substring(0, j)))
							--j;
						if (j>0){
							if (r.length() > 0){
								p = p + r +',';
								r = "";
							}
							p = p + t.substring(0,j) + ',';
							t = t.substring(j, t.length());
						}
						else{
							r = r + t.charAt(0);
							t = t.substring(1);
						}
					}
				}
				else t = t + s.charAt(i);
			outKey.set(q[0] + " <" + p + "> " + q[2] + " .");
			context.write(outKey, outValue);
		};
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			wordMap = WordTools.wordMap(context.getConfiguration());
		};
	}
	
	public void run(String dataSet, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = WordTools.wordConf();
		Job job = new Job( conf, "DI:Preprocessing#Get_P_Text_" + dataSet );
		job.setJarByClass(GetPropertyText.class);
		job.setNumReduceTasks( reducerNum );
		
		FileInputFormat.addInputPath( job, new Path(Vars.LiteralObject + dataSet) );		
		
		job.setMapperClass( GetPropertyNameMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setReducerClass( Reducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.PropertyText + "/" + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
