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

public class DisCalc {
	public static class DisCalcMapper extends Mapper<Object, Text, Text, Text>{
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
	
	public static class DisCalcReducer extends Reducer<Text, Text, Text, NullWritable>{
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
			ArrayList<Double> tmp = new ArrayList<Double>();
			for (int i=0; i<a.length(); ++i)
				tmp.add(a.getDouble(i));
			Collections.sort(tmp);
			for (int i=0; i<tmp.size(); ++i)
				if (i == 0) 
					ret.add(tmp.get(i));
				else if (NumTools.cmp(tmp.get(i), tmp.get(i-1))>0)
					ret.add(tmp.get(i));
			return ret;
		}
		
		double countDisCalc(int a, int b){
			double initSim = 0.5;
			double x = 0;
			double y = 0;
			for (int i=0; i<a; ++i)
				if (x < 0.1) x = initSim;
				else x += (1 - x) / 2;
			for (int i=0; i<b; ++i)
				if (y < 0.1) y = initSim;
				else y += (1 - y) / 2;
			return x/y;
		}
		
		double countDis(JSONArray j1, JSONArray j2) throws JSONException{
			if (nullSim(j1.length(), j2.length()) > 0)
				return nullSim(j1.length(), j2.length());
			int match = 0;
			HashSet<String> set = new HashSet<String>();
			for (int i=0; i<j1.length(); ++i)
				set.add(j1.getString(i));
			for (int i=0; i<j2.length(); ++i)
				if (set.contains(j2.getString(i)))
					match++;
			return countDisCalc(match, (j1.length() + j2.length())/2);
		}
		
		double simLabelDis(JSONArray j1, JSONArray j2) throws JSONException{
			if (nullSim(j1.length(), j2.length()) > 0)
				return nullSim(j1.length(), j2.length());
			double m = 0;
			double n = 0;
			for (int i=0; i<j1.length(); ++i){
				if (map2.containsKey(j1.get(i)))
					n += docNum / wordCount.get(j1.get(i));
				m += docNum / wordCount.get(j1.get(i));
			}
			for (int i=0; i<j2.length(); ++i){
				if (map1.containsKey(j2.get(i)))
					n += docNum / wordCount.get(j2.get(i));
				m += docNum / wordCount.get(j2.get(i));
			}
			return n / m;
		}
		
		double numDis(List<Double> l1, List<Double> l2){
			if (nullSim(l1.size(), l2.size()) > 0)
				return nullSim(l1.size(), l2.size());
			int match = 0;
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
			return countDisCalc(match, (l1.size() + l2.size())/2);
		}
		
		double sqr(double x){
			return x*x;
		}
		
		double tfidf(double count, double sum, double docNum, double wCount){
			return count / sum * Math.log(docNum / wCount);
		}
		
		double cosDis(){
			if (nullSim(map1.size(), map2.size()) > 0)
				return nullSim(map1.size(), map2.size());
			double n = 0;
			double m = 0;
			for (String i:map1.keySet()){
				if (map2.containsKey(i))
					n = n + tfidf(map1.get(i), sum1, docNum, wordCount.get(i)) *
						tfidf(map2.get(i) ,sum2 ,docNum, wordCount.get(i));
				m = m + sqr(tfidf(map1.get(i), sum1, docNum, wordCount.get(i)));
			}
			n /= Math.sqrt(m);
			m = 0;
			for (String i:map2.keySet())
				m = m + sqr(tfidf(map2.get(i) ,sum2 ,docNum, wordCount.get(i)));
			return n / Math.sqrt(m);
		}
		
		double idfDis(){
			if (nullSim(map1.size(), map2.size()) > 0)
				return nullSim(map1.size(), map2.size());
			double n = 0;
			double m = 0;
			for (String i:map1.keySet()){
				if (map2.containsKey(i))
					n = n + sqr(docNum / wordCount.get(i));
				m = m + sqr(docNum / wordCount.get(i));
			}
			n /= Math.sqrt(m);
			m = 0;
			for (String i:map2.keySet())
				m = m + sqr(docNum / wordCount.get(i));
			return n / Math.sqrt(m);
		}
		
		double topIdfDis(){
			int topN = 5;
			if (nullSim(map1.size(), map2.size()) > 0)
				return nullSim(map1.size(), map2.size());
			ArrayList<WordIdf> l1 = new ArrayList<WordIdf>();
			ArrayList<WordIdf> l2 = new ArrayList<WordIdf>();
			double minum = docNum;
			for (String i:map1.keySet()){
				double idf = docNum / wordCount.get(i); 
				l1.add(new WordIdf(i, idf));
				minum = NumTools.min(minum, idf);
			}
			for (String i:map2.keySet()){
				double idf = docNum / wordCount.get(i); 
				l2.add(new WordIdf(i, idf));
				minum = NumTools.min(minum, idf);
			}
			Collections.sort(l1);
			Collections.sort(l2);
			double m = 0;
			double n = 0;
			HashSet<String> tmpSet = new HashSet<String>();
			for (int i=0; i<topN && i<l1.size(); ++i){
				if (map2.containsKey(l1.get(i).word)){
					n += l1.get(i).idf;
					m += l1.get(i).idf;
				}
				else m += minum; 
				tmpSet.add(l1.get(i).word);
			}
			for (int i=0; i<topN && i<l2.size(); ++i)
				if (!tmpSet.contains(l2.get(i).word) && (map1.containsKey(l2.get(i).word))){
					n += l2.get(i).idf;
					m += l2.get(i).idf;
				}
				else m += minum;
			return n / m;
		}
		
		double editSim(String a, String b){
			int[][] f = new int[a.length()+1][b.length()+1];
			for (int i=0; i<=a.length(); ++i)
				for (int j=0; j<=b.length(); ++j)
					if (i==0 && j==0) f[i][j] = 0;
					else if (i==0) f[i][j] = j;
					else if (j==0) f[i][j] = i;
					else{
						f[i][j] = Math.min(Math.min(f[i-1][j], f[i][j-1]), f[i-1][j-1]) + 1;
						if (a.charAt(i-1) == b.charAt(j-1))
							f[i][j] = Math.min(f[i][j], f[i-1][j-1]);
					}
			return 1. - 1.*f[a.length()][b.length()]/Math.max(a.length(), b.length());
		}
		
		String dis(JSONObject j1, JSONObject j2) throws JSONException{
			String ret = "";

			sum1 = sum2 = 0;
			map1.clear();
			map2.clear();
			collect1(j1.getJSONArray(Vars.textShort));
			collect2(j2.getJSONArray(Vars.textShort));
			ret = ret + idfDis() + ",";			
			ret = ret + topIdfDis() + ",";
			collect1(j1.getJSONArray(Vars.textMiddle));
			collect2(j2.getJSONArray(Vars.textMiddle));
			collect1(j1.getJSONArray(Vars.label));
			collect2(j2.getJSONArray(Vars.label));
			ret = ret + idfDis() + ",";
			ret = ret + topIdfDis() + ",";
			collect1(j1.getJSONArray(Vars.property));
			collect2(j2.getJSONArray(Vars.property));
			collect1(j1.getJSONArray(Vars.textLong));
			collect2(j2.getJSONArray(Vars.textLong));
			ret = ret + cosDis() + ",";
			ret = ret + idfDis() + ",";
			ret = ret + topIdfDis() + ",";
			ret = ret + editSim(j1.getString(Vars.labelString), j2.getString(Vars.labelString)) + ",";			
			ret = ret + countDis(j1.getJSONArray(Vars.label), j2.getJSONArray(Vars.label)) + ",";
			ret = ret + numDis(getDoubleList(j1.getJSONArray(Vars.date)), 
					getDoubleList(j2.getJSONArray(Vars.date))) + ",";
			ret = ret + numDis(getDoubleList(j1.getJSONArray(Vars.num)),
					getDoubleList(j2.getJSONArray(Vars.num))) + ",";
			ret = ret + countDis(j1.getJSONArray(Vars.link), j2.getJSONArray(Vars.link)) + ",";
			ret = ret + simLabelDis(j1.getJSONArray(Vars.label), j2.getJSONArray(Vars.label));
			return ret;
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
			int index=0;
			for (Text value:values)
				q[index++] = value.toString();
			if (index != 2)
				return;
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
		Job job = new Job( conf, "DI:Match#Dis_Calc_" + dataSet );
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(DisCalc.class);
		
		FileInputFormat.addInputPath(job, new Path(Vars.IdPreMatch + dataSet));
		
		job.setMapperClass( DisCalcMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( DisCalcReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.DisCalc + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}

class WordIdf implements Comparable<WordIdf>{
	public String word;
	public double idf;
	
	public WordIdf(String word, double idf){
		this.word = word;
		this.idf = idf;
	}
	
	@Override
	public int compareTo(WordIdf o) {
		return -NumTools.cmp(idf, o.idf);
	}			
}
