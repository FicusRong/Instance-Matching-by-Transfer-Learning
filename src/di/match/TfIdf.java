package di.match;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import di.tools.Vars;
import di.tools.WordTools;

public class TfIdf {
	static int M = 30;
	
	public static class TfIdfMapper extends Mapper<Object, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		HashMap<String, Integer> wordCount;
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		int sum;
		double docNum;
		
		void collect(JSONArray a) throws JSONException{
			for (int i=0; i<a.length(); ++i){
				String s = (String) a.get(i);
				if (!wordCount.containsKey(s))
					continue;
				++sum;
				if (map.containsKey(s))
					map.put(s, map.get(s)+1);
				else map.put(s, 1);
			}
		}
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			try {
				JSONObject json = new JSONObject(value.toString());
				JSONObject output = new JSONObject();
				JSONObject words = new JSONObject();
				output.put(Vars.uri, json.get(Vars.uri));
				map.clear();
				sum = 0;
				collect(json.getJSONArray(Vars.textLong));
				collect(json.getJSONArray(Vars.textMiddle));
				collect(json.getJSONArray(Vars.textShort));
				collect(json.getJSONArray(Vars.property));
				for (String i:map.keySet()){
//					double tfidf = (double)map.get(i)/sum * Math.log(docNum/wordCount.get(i));
//					words.put(i, tfidf);
					double d = Math.log(map.get(i)+1) * docNum/wordCount.get(i);
					words.put(i, d);
				}
				output.put(Vars.wordList, words);
				outKey.set(output.toString());
				context.write(outKey, outValue);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		};
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			Configuration conf = context.getConfiguration();
			String dataSet = conf.get(Vars.dataSet);
			wordCount = WordTools.wordCount(dataSet);
			docNum = Vars.docNum(dataSet);
		};
	}
	
	public void run(String[] dataSets, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		String dataSet = Vars.combDataSets(dataSets);
		Configuration conf = new Configuration();
		conf.set(Vars.dataSet, dataSet);
		
		Job job = new Job( conf, "DI:Match#TF_IDF_" + dataSet);
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(TfIdf.class);
		
		for (String i:dataSets)
			FileInputFormat.addInputPath( job, new Path(Vars.Json + i) );
		
		job.setMapperClass( TfIdfMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setReducerClass( Reducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.TfIdf + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
