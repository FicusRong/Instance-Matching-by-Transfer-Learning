package di.match;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import di.tools.Vars;

public class PreMatch {
	static class Pair implements Comparable<Pair>{
		String s;
		double d;
		public Pair(){}
		public Pair(String s, double d) {
			this.s = s;
			this.d = d;
		}
		@Override
		public int compareTo(Pair o) {
			if (d > o.d)
				return -1;
			if (d < o.d)
				return 1;
			return 0;
		}		
	}
	
	public static class PreMatchMapper extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		ArrayList<Pair> list = new ArrayList<PreMatch.Pair>();
		int topN = 0;
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			try {
				JSONObject json = new JSONObject(value.toString());
				outValue.set(json.getString(Vars.uri));
				if (json.has(Vars.wordList)){
					JSONObject wordList = json.getJSONObject(Vars.wordList);
					list.clear();
					for (Iterator<String> i = wordList.keys(); i.hasNext();){
						String t = i.next();
						list.add(new Pair(t, wordList.getDouble(t)));
					}
					Collections.sort(list);
					for (int i=0; i<topN && i<list.size(); ++i){
						outKey.set(list.get(i).s);
						context.write(outKey, outValue);
					}
				}
				else{
					JSONArray a = json.getJSONArray(Vars.label);
					for (int i=0; i<a.length(); ++i){
						outKey.set(a.getString(i));
						context.write(outKey, outValue);
					}
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}			
		};
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			Configuration conf = context.getConfiguration();
			topN = Vars.preMatchTopWords(conf.get(Vars.dataSet + 1) + "_" + conf.get(Vars.dataSet + 2));
		};
	}
	
	public static class PreMatchReducer extends Reducer<Text, Text, Text, NullWritable>{
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
			if (set1.size()*set2.size() < M*M)
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
		Job job = new Job( conf, "DI:Match#Pre_Match_" + dataSet );
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(PreMatch.class);
		
		FileInputFormat.addInputPath( job, new Path(Vars.Json + dataSet1) );
		FileInputFormat.addInputPath( job, new Path(Vars.Json + dataSet2) );
		FileInputFormat.addInputPath(job, new Path(Vars.TfIdf + dataSet));
		
		job.setMapperClass( PreMatchMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( PreMatchReducer.class );
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
