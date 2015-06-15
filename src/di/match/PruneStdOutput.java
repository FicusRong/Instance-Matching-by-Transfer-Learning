package di.match;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
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

public class PruneStdOutput {
	public static class PruneStdOutputMapper extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		ArrayList<String> train = new ArrayList<String>();
		HashSet<String> trainSet = new HashSet<String>();
		RandomForest tree;
		int randomSeed; 
		int missN = 0;
		String dataSet;
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			dataSet = context.getConfiguration().get(Vars.dataSet);
			randomSeed = context.getConfiguration().getInt(Vars.randomSeed, 0);
			Random random = new Random(randomSeed);
			TextInputFormat reader = new TextInputFormat();
			Job tempJob = new Job(new Configuration());
			FileInputFormat.addInputPath(tempJob, new Path(Vars.StandardOutput + dataSet));
			List<InputSplit> inputSplits = reader.getSplits(tempJob);
			TaskAttemptID tempID = new TaskAttemptID(new TaskID("tempID", 0, true, 1), 0);
			TaskAttemptContext tempContext = new TaskAttemptContext(tempJob.getConfiguration(), tempID);
			for (InputSplit split : inputSplits) {
				LineRecordReader recordReader = (LineRecordReader) reader.createRecordReader(split, tempContext);
				recordReader.initialize(split, tempContext);
				while(recordReader.nextKeyValue()){
					String s = recordReader.getCurrentValue().toString();
					if (random.nextInt(100) < Vars.trainingRate){
						train.add(s);
						trainSet.add(s);
					}
				}
			}
			
			Boosting.m = train.size();
			Boosting.trainData = new int[Boosting.m][Vars.AttrNum];
			Boosting.weight = new double[Boosting.m];
			double posN = 0;
			for (int i=0; i<Boosting.m; ++i){
				String[] q = train.get(i).split(",");
				for (int j=0; j<Vars.AttrNum; ++j)
					if (j == Vars.AttrNum-1)
						Boosting.trainData[i][j] = Integer.parseInt(q[j]);
					else Boosting.trainData[i][j] = (int) Math.round(Double.parseDouble(q[j]) * Vars.prec);
				Boosting.weight[i] = 1;
				if (Boosting.trainData[i][Vars.AttrNum-1] == 1) posN += 1;
			}
			if (posN > 0){
				double k = 1.0 * (Boosting.m - posN) / posN;
				for (int i=0; i<Boosting.m; ++i)
					if (Boosting.trainData[i][Vars.AttrNum-1] == 1)
						Boosting.weight[i] = k;
			}
			ArrayList<Integer> trainList = new ArrayList<Integer>();
			for (int i=0; i<Boosting.m; ++i)
				trainList.add(i);
			tree = new RandomForest(Vars.pruneRandomTreeN, trainList, randomSeed);
		};
		
		@Override
		protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String[] q = value.toString().split(",");
			int[] test = new int[Vars.AttrNum]; 
			Random random = new Random();
			for (int j=0; j<Vars.AttrNum; ++j)
				if (j == Vars.AttrNum-1)
					test[j] = Integer.parseInt(q[j]);
				else test[j] = (int) Math.round(Double.parseDouble(q[j]) * Vars.prec);
			outKey.set("output");
			if (trainSet.contains(value.toString())){
				outValue.set("_" + value.toString());
				if (value.toString().endsWith("1"))
					context.write(outKey, outValue);
				else if (random.nextInt(100)<Vars.prunePercent(dataSet))
					context.write(outKey, outValue);
				return;
			}
			if (tree.predicateNum(test) > 0)
				context.write(outKey, value);
			else if (test[Vars.AttrNum-1] == 1){
				outValue.set("!" + value.toString());
				context.write(outKey, outValue);
			}
		};
		
		@Override
		protected void cleanup(Context context) throws IOException ,InterruptedException {
			outKey.set("miss");
			outValue.set("" + missN);
			context.write(outKey, outValue);
		};
	}

	public static class PruneStdOutputReducer extends Reducer<Text, Text, Text, NullWritable>{
		NullWritable outValue = NullWritable.get();
		int missN = 0;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
			if (key.toString().equals("output"))
				for (Text value:values)
					context.write(value, outValue);
			else{
				for (Text value:values)
					missN += Integer.parseInt(value.toString());
				System.out.println("Miss: " + missN);
			}
		};
	}
	
	public void run(String dataSet1, String dataSet2, int randomSeed) throws IOException, InterruptedException, ClassNotFoundException{
		String dataSet = dataSet1 + "_" + dataSet2;		
		Configuration conf = new Configuration();
		conf.set(Vars.dataSet, dataSet);
		conf.setInt(Vars.randomSeed, randomSeed);
		Job job = new Job( conf, "DI:Match#Prune_Std_Output_" + dataSet );
		job.setNumReduceTasks( 1 );
		job.setJarByClass(PruneStdOutput.class);
		
		FileInputFormat.addInputPath(job, new Path(Vars.StandardOutput + dataSet));
		
		job.setMapperClass( PruneStdOutputMapper.class );
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( PruneStdOutputReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );
		
		String outputPath = Vars.PruneStdOutput + dataSet;
		Path output = new Path(outputPath);
	    FileSystem fs = FileSystem.get(new Configuration());
	    if(fs.exists(output))
	    	fs.delete(output, true);
		FileOutputFormat.setOutputPath( job, new Path(outputPath) );

		job.waitForCompletion( true );
	}
}
