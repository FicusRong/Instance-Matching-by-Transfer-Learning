package di.match;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

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

import di.tools.NumTools;
import di.tools.Vars;
import di.tools.WordTools;

public class IdfVector {
	public static class IdfVectorMapper extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		String dataSet1, dataSet2;
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String s = value.toString();
			outKey.set(s.substring(0, s.indexOf(" ")));
			outValue.set(s.substring(s.indexOf(" ")+1));
			context.write(outKey, outValue);
		};
	}
	
	public static class IdfVectorReducer extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		String dataSet1, dataSet2;
		String[] q = new String[2];
		HashMap<String, Integer> wordCount;
		double docNum;
		int sum1, sum2;
		HashMap<String, Integer> map1 = new HashMap<String, Integer>();
		HashMap<String, Integer> map2 = new HashMap<String, Integer>();
		
		double nullSim(int n, int m){
			double t = 0.1;
			if (n==0 && m==0)
				return t;
			if (n==0)
				return t / (1+m);
			if (m==0)
				return t / (1+n);
			return -1;
		}
		
		void collect1(JSONArray a) throws JSONException{
			for (int i=0; i<a.length(); ++i){
				String s = (String) a.get(i);
				if (!wordCount.containsKey(s))
					continue;
				++sum1;
				if (map1.containsKey(s))
					map1.put(s, map1.get(s)+1);
				else map1.put(s, 1);
			}
		}
		
		void collect2(JSONArray a) throws JSONException{
			for (int i=0; i<a.length(); ++i){
				String s = (String) a.get(i);
				if (!wordCount.containsKey(s))
					continue;
				++sum2;
				if (map2.containsKey(s))
					map2.put(s, map2.get(s)+1);
				else map2.put(s, 1);
			}
		}
		
		ArrayList<Double> getDoubleList(JSONArray a) throws JSONException{
			ArrayList<Double> ret = new ArrayList<Double>();
			for (int i=0; i<a.length(); ++i)
				ret.add(a.getDouble(i));
			Collections.sort(ret);
			return ret;
		}
		
		double labelDis(JSONArray j1, JSONArray j2) throws JSONException{
			if (nullSim(j1.length(), j2.length()) > 0)
				return nullSim(j1.length(), j2.length());
			double match = 0;
			double sum = 0;
			double initSim = 0.5;
			for (int i=0; i<j1.length()+j2.length(); i+=2)
				if (sum < 0.1) sum = initSim;
				else sum += (1 - sum) / 2;
			HashSet<String> set = new HashSet<String>();
			for (int i=0; i<j1.length(); ++i)
				set.add(j1.getString(i));
			for (int i=0; i<j2.length(); ++i)
				if (set.contains(j2.getString(i)))
					if (match < 0.1) match = initSim;
					else match += (1 - match) / 2;
			return match / sum;
		}
		
		double numDis(List<Double> l1, List<Double> l2){
			if (nullSim(l1.size(), l2.size()) > 0)
				return nullSim(l1.size(), l2.size());
			double match = 0;
			int i = 0;
			int j = 0;
			while(i<l1.size() && j<l2.size()){
				int k = NumTools.cmp(l1.get(i), l2.get(j));
				if (k == 0){
					++match;
					++i;
					++j;
				}
				else if (k > 0) ++j;
				else ++i;
			}
			return match / Math.sqrt(l1.size() * l2.size());
		}
		
		double sqr(double x){
			return x*x;
		}
		
		int index(String s){
			double t = wordCount.get(s);
			if (t/docNum<0.1)
				return (int) Math.floor(t/docNum*1000);
			else return (int) Math.floor(t/docNum*100) + 90;
		}
		
		String dis(JSONObject j1, JSONObject j2) throws JSONException{
			String ret = "";
			ret = ret + "1:" + labelDis(j1.getJSONArray(Vars.label), j2.getJSONArray(Vars.label));
			ret = ret + " 2:" + numDis(getDoubleList(j1.getJSONArray(Vars.num)),
					getDoubleList(j2.getJSONArray(Vars.num)));
			ret = ret + " 3:" + numDis(getDoubleList(j1.getJSONArray(Vars.date)), 
					getDoubleList(j2.getJSONArray(Vars.date)));
			sum1 = sum2 = 0;
			map1.clear();
			map2.clear();
			collect1(j1.getJSONArray(Vars.textShort));
			collect2(j2.getJSONArray(Vars.textShort));
			collect1(j1.getJSONArray(Vars.label));
			collect2(j2.getJSONArray(Vars.label));
			int[] a = new int[190];
			int[] b = new int[190];
			for (String i:map1.keySet())
				if (map2.containsKey(i))
					a[index(i)] += map1.get(i) + map2.get(i);
				else b[index(i)] += map1.get(i);
			for (String i:map2.keySet())
				if (!map1.containsKey(i))
					b[index(i)] += map2.get(i);
			for (int i=0; i<190; ++i)
				if (a[i] > 0)
					ret = ret + " " + (i+4) + ":" + a[i];
			for (int i=0; i<190; ++i)
				if (b[i] > 0)
					ret = ret + " " + (i+194) + ":" + b[i];			
			collect1(j1.getJSONArray(Vars.property));
			collect2(j2.getJSONArray(Vars.property));
			collect1(j1.getJSONArray(Vars.textMiddle));
			collect2(j2.getJSONArray(Vars.textMiddle));
			collect1(j1.getJSONArray(Vars.textLong));
			collect2(j2.getJSONArray(Vars.textLong));
			for (int i=0; i<190; ++i)
				a[i] = b[i] = 0;
			for (String i:map1.keySet())
				if (map2.containsKey(i))
					a[index(i)] += map1.get(i) + map2.get(i);
				else b[index(i)] += map1.get(i);
			for (String i:map2.keySet())
				if (!map1.containsKey(i))
					b[index(i)] += map2.get(i);
			for (int i=0; i<190; ++i)
				if (a[i] > 0)
					ret = ret + " " + (i+384) + ":" + a[i];
			for (int i=0; i<190; ++i)
				if (b[i] > 0)
					ret = ret + " " + (i+574) + ":" + b[i];		
			return ret;
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
			int index=0;
			for (Text value:values)
				q[index++] = value.toString();
			try {
				JSONObject j1 = new JSONObject(q[0]);
				JSONObject j2 = new JSONObject(q[1]);
				String s;
				if (j1.getString(Vars.uri).startsWith(Vars.uriPrefix(dataSet1)))
					s = j1.getString(Vars.uri) + " " + j2.getString(Vars.uri);
				else s = j2.getString(Vars.uri) + " " + j1.getString(Vars.uri);
				outKey.set(s + " " + dis(j1, j2));
				context.write(outKey, outValue);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		};
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			Configuration conf = context.getConfiguration();
			dataSet1 = conf.get(Vars.dataSet + 1);
			dataSet2 = conf.get(Vars.dataSet + 2);
			String dataSet = dataSet1 + "_" + dataSet2;
			wordCount = WordTools.wordCount(dataSet);
			docNum = Vars.docNum(dataSet);
		};
	}
	
	public void run(String dataSet1, String dataSet2, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		String dataSet = dataSet1 + "_" + dataSet2;		
		Configuration conf = new Configuration();
		conf.set(Vars.dataSet+1, dataSet1);
		conf.set(Vars.dataSet+2, dataSet2);
		Job job = new Job( conf, "DI:Match#Idf_Vector_" + dataSet );
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(DisCalc.class);
		
		FileInputFormat.addInputPath(job, new Path(Vars.IdPreMatch + dataSet));
		
		job.setMapperClass( IdfVectorMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( IdfVectorReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.IdfVector + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
