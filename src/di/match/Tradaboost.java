package di.match;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

import di.tools.Vars;

public class Tradaboost extends Boosting{
	public static class TradaboostMapper extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		ArrayList<String> test = new ArrayList<String>();
		ArrayList<String> train = new ArrayList<String>();
		ArrayList<String> error = new ArrayList<String>();
		int randomSeed; 
		Random random;
		String dataSet;
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			String taskId = context.getConfiguration().get("mapred.task.id")
				.substring(context.getConfiguration().get("mapred.task.id").indexOf("_m_") + 3);
			taskId = taskId.substring(0, taskId.indexOf("_"));
			long mapID = Long.valueOf(taskId).longValue();
			System.out.println("MapID: " + mapID);
			dataSet = context.getConfiguration().get(Vars.dataSet);
			randomSeed = context.getConfiguration().getInt(Vars.randomSeed, 0);
			random = new Random(randomSeed);
			TextInputFormat reader = new TextInputFormat();
			Job tempJob = new Job(new Configuration());
			String inputPath = Vars.StandardOutput + dataSet;
			if (Vars.prune(dataSet))
				inputPath = Vars.PruneStdOutput + dataSet;
			FileInputFormat.addInputPath(tempJob, new Path(inputPath));
			List<InputSplit> inputSplits = reader.getSplits(tempJob);
			TaskAttemptID tempID = new TaskAttemptID(new TaskID("tempID", 0, true, 1), 0);
			TaskAttemptContext tempContext = new TaskAttemptContext(tempJob.getConfiguration(), tempID);
			for (InputSplit split : inputSplits) {
				LineRecordReader recordReader = (LineRecordReader) reader.createRecordReader(split, tempContext);
				recordReader.initialize(split, tempContext);
				while(recordReader.nextKeyValue()){
					String s = recordReader.getCurrentValue().toString();
					if (Vars.prune(dataSet))
						if (s.startsWith("_")) train.add(s.substring(1));
						else if (s.startsWith("!")) error.add(s.substring(1));
						else test.add(s);
					else if (random.nextInt(100) < Vars.trainingRate) train.add(s);
					else test.add(s);
				}
			}
			n = train.size();
		};
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			if (Vars.useSourceDomain)
				if (random.nextInt(100) < Vars.sourceRate(dataSet)){
					String s = value.toString();
					if (s.startsWith("_"))
						s = s.substring(1);
					train.add(s);
				}
		};
		
		@Override
		protected void cleanup(Context context) throws IOException ,InterruptedException {
			m = train.size();
			trainData = new int[m][Vars.AttrNum];
			weight = new double[m];
			double posN = 0;
			for (int i=0; i<m; ++i){
				String[] q = train.get(i).split(",");
				for (int j=0; j<Vars.AttrNum; ++j)
					if (j == Vars.AttrNum-1)
						trainData[i][j] = Integer.parseInt(q[j]);
					else trainData[i][j] = (int) Math.round(Double.parseDouble(q[j]) * Vars.prec);
				weight[i] = 1;
				if (i < n && trainData[i][Vars.AttrNum-1] == 1) posN += 1;
			}
			if (posN > 0){
				double k = 1.0 * (n - posN) / posN;
				for (int i=0; i<n; ++i)
					if (trainData[i][Vars.AttrNum-1] == 1)
						weight[i] = k;
			}
			ArrayList<Integer> trainList = new ArrayList<Integer>();
			for (int i=0; i<m; ++i)
				trainList.add(i);
			
			RandomForest[] tree = new RandomForest[Vars.N];
//			C45Tree[] tree = new C45Tree[Vars.N];
//			RandomTree[] tree = new RandomTree[Vars.N];
			double[] beta = new double[Vars.N];
			double betaD = 1. / (1 + Math.sqrt(2 * Math.log(1.*(m-n)/Vars.N))); 
			for (int t=0; t<Vars.N; ++t){
				System.out.println("Tradaboost round: " + t);
//				for (int i=0; i<100; ++i)
//					System.out.print(" "+weight[i]);
//				System.out.println(",");
//				for (int i=0; i<100; ++i)
//					System.out.print(" "+weight[n+i]);
//				System.out.println(",");
				
				context.write(new Text(""), new Text(""));
				tree[t] = new RandomForest(Vars.TradaboostRandomTreeN, trainList, randomSeed);
//				tree[t] = new C45Tree(trainList);
//				tree[t] = new RandomTree(trainList, 10);
				double err = 0;
				double sum = 0;
				for (int i=0; i<n; ++i){
					sum += weight[i];
					err += weight[i] * Math.abs(tree[t].predicate(trainData[i]) - trainData[i][Vars.AttrNum-1]);
				}
				err /= sum;
				beta[t] = err / (1-err);
				for (int i=0; i<n; ++i)
					weight[i] = weight[i] * Math.pow(beta[t], -Math.abs(tree[t].predicate(trainData[i])- trainData[i][Vars.AttrNum-1]));
				for (int i=n; i<m; ++i)
					weight[i] = weight[i] * Math.pow(betaD, Math.abs(tree[t].predicate(trainData[i])- trainData[i][Vars.AttrNum-1]));
			}
			double y = 1;
			for (int j=Vars.N/2; j<Vars.N; ++j)
//			for (int j=0; j<Vars.N; ++j)
				y *= Math.pow(beta[j], -0.5);
			System.out.println("testSize: " + test.size());
			int r1 = 0, r0 = 0, w1 = 0,w0 = 0;
			String ans = "";
			String pre = "";
			for (int i=0; i<test.size(); ++i){
				if (i > 0 && i%10000 == 0){
					System.out.println("tested: " + i);
					context.write(new Text(""), new Text(""));
				}
				String[] tmp = test.get(i).split(",");
				int[] q = new int[Vars.AttrNum];
				for (int j=0; j<Vars.AttrNum; ++j)
					if (j == Vars.AttrNum-1)
						q[j] = Integer.parseInt(tmp[j]);
					else q[j] = (int) Math.round(Double.parseDouble(tmp[j]) * Vars.prec);
				double x = 1;
				for (int j=Vars.N/2; j<Vars.N; ++j)
//				for (int j=0; j<Vars.N; ++j)
					x *= Math.pow(beta[j], -tree[j].predicate(q));
				int k = 1;
				if (x < y) k = 0;
				if (i > 0){
					ans = ans + ",";
					pre = pre + ",";
				}
				ans = ans + q[Vars.AttrNum - 1];
				pre = pre + k;
			}
			for (String s:error){
				ans = ans + "," + s.charAt(s.length()-1);
				pre = pre + ",2";
			}
			outKey.set("ans");
			outValue.set(ans);
			context.write(outKey, outValue);
			outKey.set("pre");
			outValue.set(pre);
			context.write(outKey, outValue);
		};
	}
	
	public static class TradaboostReducer extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		NullWritable outValue = NullWritable.get();
		int r1 = 0, r0 = 0, w1 = 0, w0 = 0;
		ArrayList<String> ans = new ArrayList<String>();
		ArrayList<String> pre = new ArrayList<String>();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
			if (key.toString().equals("ans"))
				for (Text value : values) ans.add(value.toString());
			else if (key.toString().equals("pre"))
				for (Text value : values) pre.add(value.toString());
		};
		
		@Override
		protected void cleanup(Context context) throws IOException ,InterruptedException {
			for (int i=1; i<ans.size(); ++i)
				if (!ans.get(i).equals(ans.get(0))){
					outKey.set("Inconsistant ans");
					context.write(outKey, outValue);
					return;
				}
			String[] a = ans.get(0).split(",");
			int[] sum = new int[a.length];
			
			for (int i=0; i<pre.size(); ++i){
				String[] b = pre.get(i).split(",");
				for (int j=0; j<b.length; ++j)
					if (b[j].equals("1"))
						sum[j]++;
			}

			for (int i=0; i<a.length; ++i)
				if (a[i].equals("1"))
					if (sum[i]*2 > ans.size()) ++r1;
					else ++w1;
				else
					if (sum[i]*2 > ans.size()) ++w0;
					else ++r0;
			outKey.set(r1 + "\t" + w1);
			context.write(outKey, outValue);
			outKey.set(w0 + "\t" + r0);
			context.write(outKey, outValue);
			outKey.set("Recall: " + 1.*r1/(r1+w1));
			context.write(outKey, outValue);
			outKey.set("Precision: " + 1.*r1/(r1+w0));
			context.write(outKey, outValue);
		};
	}
	
	public void run(String dataSet1, String dataSet2, int randomSeed) throws IOException, InterruptedException, ClassNotFoundException{
		String dataSet = dataSet1 + "_" + dataSet2;		
		Configuration conf = new Configuration();
		conf.set(Vars.dataSet, dataSet);
		conf.setInt(Vars.randomSeed, randomSeed);
		Job job = new Job( conf, "DI:Match#Tradaboost_" + dataSet );
		job.setNumReduceTasks( 1 );
		job.setJarByClass(DisCalc.class);
		
//		FileInputFormat.addInputPath(job, new Path(Vars.StandardOutput + Vars.DBP + "_" +Vars.FB));
		FileInputFormat.addInputPath(job, new Path(Vars.PruneStdOutput + Vars.SD + "_" + Vars.DM));
		
		job.setMapperClass( TradaboostMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( TradaboostReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.Tradaboost + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
