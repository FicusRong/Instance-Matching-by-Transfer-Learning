package di.preprocess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

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
import org.json.JSONString;
import org.json.JSONStringer;

import di.tools.NumTools;
import di.tools.Vars;
import di.tools.TripleUtil;
import di.tools.WordTools;

public class TripleToJson {
	public static final int textL = 10;
	
	public static class TripleToJsonMapper extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String q[] = TripleUtil.parseTriple(value.toString());
			outKey.set(q[0]);
			outValue.set(q[1] + " " + q[2]);
			context.write(outKey, outValue);
		};
	}
	
	public static Pattern doublePrefixPattern = Pattern.compile("[0-9]+[.]?[0-9]*e?[+|-]?[0-9]*");
	public static Pattern intPattern = Pattern.compile("[0-9]+");
	
	public static String[] nextToken(String s){
		for (int i=0; i<s.length(); ++i)
			if (s.charAt(i) == ',')
				if (i>0 && Character.isDigit(s.charAt(i-1))
						&& i<s.length() && Character.isDigit(s.charAt(i+1)))
					s = s.substring(0, i) + s.substring(i+1);
		String[] ret = new String[2];
		int i = 0;
		String t = "";
		while(i<s.length() && !Character.isDigit(s.charAt(i)) && !Character.isLowerCase(s.charAt(i))) ++i;
		while(i<s.length() && (Character.isDigit(s.charAt(i)) || Character.isLowerCase(s.charAt(i))))
			t = t + s.charAt(i++);
		while(i<s.length() && doublePrefixPattern.matcher(t + s.charAt(i)).matches())
			t = t + s.charAt(i++);
		ret[0] = t;
		ret[1] = s.substring(i);
		return ret;
	}
	
	public static class TripleToJsonReducer extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		Map<String, Integer> wordMap;
		
		String check(String s){
			if (wordMap.containsKey(s))
				return "_" + wordMap.get(s);
			return s;
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
			try {
				JSONObject json = new JSONObject();
				JSONArray label = new JSONArray();
				JSONArray num = new JSONArray();
				JSONArray date = new JSONArray();
				JSONArray textLong = new JSONArray();
				JSONArray textMiddle = new JSONArray();
				JSONArray textShort = new JSONArray();
				JSONArray property = new JSONArray();
				JSONArray link = new JSONArray();
				
				int totalTriple = 0;
				json.put(Vars.uri, key.toString());
				for (Text value : values){
					totalTriple++;
					if (totalTriple > 1000)
						break;
					String p = value.toString().substring(0, value.toString().indexOf(' '));
					String o = value.toString().substring(value.toString().indexOf(' ') + 1);
					o = TripleUtil.neededLiteral(o.toLowerCase());
					if (o.startsWith("<")){
						link.put(o);
						continue;
					}
					if (p.equals(Vars.labelProperty)){
						String t = "";
						for (int i=0; i<o.length(); ++i){
							while (Character.isDigit(o.charAt(i)) || Character.isLowerCase(o.charAt(i)))
								t = t + o.charAt(i++);
							if (t.length() > 0){
								label.put(check(t));
								t = "";
							}
						}
						if (t.length() > 0)
							label.put(check(t));
						json.put(Vars.labelString, o);
						continue;
					}
					ArrayList<String> objectTokens = new ArrayList<String>();
					for (String s=o; s.length()>0; ){
						String[] tokens = nextToken(s);
						s = tokens[1];
						try {
							double d = Double.parseDouble(tokens[0]);
							boolean flag = true;
							for (int i=0; i<num.length(); ++i)
								if (NumTools.cmp(d, num.getDouble(i)) == 0){
									flag = false;
									break;
								}
							if (flag)
								num.put(d);
						} catch (Exception e) {
							objectTokens.add(tokens[0]);
						}
						try {
							int year = Integer.parseInt(tokens[0]);
							if (year>1500 && year<2100){
								boolean flag = true;
								for (int i=0; i<date.length(); ++i)
									if (year == date.getInt(i)){
										flag = false;
										break;
									}
								if (flag)
									date.put(year);
							}
						} catch (Exception e) {}
					}
					if (objectTokens.size() == 1)
						for (String i:objectTokens)
							textShort.put(i);
					else if (objectTokens.size() < textL)
						for (String i:objectTokens)
							textMiddle.put(i);
					else for (String i:objectTokens)
						textLong.put(check(i));
					String q[] = p.substring(1, p.length()-1).split("[,]");
					for (String i:q) property.put(check(i));
				}
				if (totalTriple > 1000)
					return;
				json.put(Vars.label, label);
				json.put(Vars.num, num);
				json.put(Vars.date, date);
				json.put(Vars.textLong, textLong);
				json.put(Vars.textMiddle, textMiddle);
				json.put(Vars.textShort, textShort);
				json.put(Vars.property, property);
				json.put(Vars.link, link);
				if (json.toString().length() < 10000000){
					outKey.set(json.toString());
					context.write(outKey, outValue);
				}				
			} catch (JSONException e) {
				e.printStackTrace();
			}
		};
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			wordMap = WordTools.wordMap(context.getConfiguration());
		};
	}
	
	public void run(String dataSet, int reducerNum) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = WordTools.wordConf();
		Job job = new Job( conf, "DI:Preprocessing#Triple_to_Json_" + dataSet );
		job.setNumReduceTasks( reducerNum );
		job.setJarByClass(TripleToJson.class);
		
		FileInputFormat.addInputPath( job, new Path(Vars.MergedBN + dataSet) );
		
		job.setMapperClass( TripleToJsonMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( TripleToJsonReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.Json + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
