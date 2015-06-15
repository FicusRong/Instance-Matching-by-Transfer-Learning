package di.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class WordTools {
	public static HashMap <String, Integer> wordCount(String dataSet) throws IOException{
		HashMap <String, Integer> ret = new HashMap<String, Integer>();		
		TextInputFormat reader = new TextInputFormat();
		Job tempJob = new Job(new Configuration());
		FileInputFormat.addInputPath(tempJob, new Path(Vars.WordCount + dataSet));
		List<InputSplit> inputSplits = reader.getSplits(tempJob);
		TaskAttemptID tempID = new TaskAttemptID(new TaskID("tempID", 0, true, 1), 0);
		TaskAttemptContext tempContext = new TaskAttemptContext(tempJob.getConfiguration(), tempID);
		for (InputSplit split : inputSplits) {
			LineRecordReader recordReader = (LineRecordReader) reader.createRecordReader(split, tempContext);
			recordReader.initialize(split, tempContext);
			while(recordReader.nextKeyValue()){
				String s = recordReader.getCurrentValue().toString();
				ret.put(s.substring(0, s.indexOf(' ')),
						Integer.parseInt(s.substring(s.indexOf(' ')+1)));
			}
		}
		return ret;
	}
	
	public static String readWord(String s){
		String t = "";
		int i = 0;
		while(i<s.length() && !Character.isLowerCase(s.charAt(i))) ++i;
		while(i<s.length() && Character.isLowerCase(s.charAt(i))){
			t = t + s.charAt(i);
			++i;
		}
		return t;
	}
	
	public static HashMap<String, Integer> wordMap() throws IOException{
		FileReader fileReader = new FileReader(Vars.poseidon + "/" + Vars.wordList);
		BufferedReader bufferReader = new BufferedReader(fileReader);
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		while (true){
			String s = bufferReader.readLine();
			if (s == null)
				break;
			String q[] = s.split("[ ]");
			map.put(q[0], Integer.parseInt(q[1]));
		}
		return map;
	}
	
	public static HashMap<String, Integer> wordMap(Configuration conf){
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for (int i=0; ; ++i){
			String merge = conf.get(Vars.wordList + i);
			if (merge == null)
				break;
			String q[] = merge.split("[,]");
			for (String s : q)
				map.put(s.substring(0, s.indexOf('_')), Integer.parseInt(s.substring(s.indexOf('_')+1)));
		}
		return map;
	}
	
	public static Configuration wordConf() throws IOException{
		Configuration conf = new Configuration();
		int n = 0;
		String merge = "";
		Map<String, Integer> wordMap = wordMap();
		for (String word : wordMap.keySet()){
			merge = merge + word + '_' + wordMap.get(word) + ',';
			if (merge.length() > Vars.MaxConfStringSize){
				conf.set(Vars.wordList + n, merge);
				++n;
				merge = "";
			}
		}
		if (merge.length() > 0)
			conf.set(Vars.wordList, merge);
		return conf;
	}
	
	public static void main(String[] args) throws IOException{
		FileReader fileReader = new FileReader(Vars.poseidonWin + "/WordListInit.txt");
		BufferedReader bufferReader = new BufferedReader(fileReader);
		String line;
		TreeMap <String, Integer> map = new TreeMap<String, Integer>();
		ArrayList <String> list = new ArrayList<String>(); 
		int n = 0;
		int m;
		while (true){
			line = bufferReader.readLine();
			if (line == null)
				break;
			if (!line.startsWith(" ") && list.size()>0){
				m = 0;
				for (String i:list)
					if (map.containsKey(i)){
						m = map.get(i);
						break;
					}
				if (m == 0)
					m = ++n;
				for (String i:list)
					map.put(i, m);
				list.clear();
			}
			while(true){
				String s = readWord(line);
				if (s.length() == 0)
					break;
				list.add(s);
				line = line.substring(line.indexOf(s) + s.length());
			}
		}
		FileWriter writer = new FileWriter(Vars.poseidonWin + "/" + Vars.wordList);
		for (String i : map.keySet())
			writer.write(i + " " + map.get(i) + "\n");
		writer.close();
	}
}
