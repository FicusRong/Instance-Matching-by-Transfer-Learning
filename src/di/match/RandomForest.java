package di.match;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import di.tools.Vars;

public class RandomForest {
	public ArrayList<RandomTree> treeList;
	
	public RandomForest(int treeNum, ArrayList<Integer> trainList, int randomSeed){
		treeList = new ArrayList<RandomTree>();
		double[] index = new double[trainList.size()];
		index[0] = Boosting.weight[trainList.get(0)];
		for (int i=1; i<trainList.size(); ++i)
			index[i] = index[i-1] + Boosting.weight[trainList.get(i)];
		
		int k = 0;
		for (int i=0; i<trainList.size(); ++i)
			if (Boosting.trainData[i][Vars.AttrNum-1] == 1) ++k;
		System.out.println(k +"  "+trainList.size());
			
		Random random = new Random(randomSeed);
		for (int i=0; i<treeNum; ++i){
			System.out.println("TreeNo: " + i);
			ArrayList<Integer> pickList = new ArrayList<Integer>();
			for (int j=0; j<trainList.size() / Vars.randomForestD; ++j){
				double tmp = random.nextDouble() * index[trainList.size()-1];
				int l = 0;
				int r = trainList.size() - 1;
				while(l<r){
					int mid = (l+r)/2;
					if (index[mid] >= tmp) r = mid;
					else l = mid + 1;
				}
				pickList.add(trainList.get(l));
			}
			treeList.add(new RandomTree(pickList, i));
		}		
	}
	
	public int predicate(int[] q){
		int tmp = 0;
		for (RandomTree tree : treeList)
			tmp += tree.predicate(q);
		if (Vars.conf * tmp * 2 > treeList.size())
			return 1;
		return 0;
	}
	
	public int predicateNum(int[] q){
		int tmp = 0;
		for (RandomTree tree : treeList)
			tmp += tree.predicate(q);
		return tmp;
	}
	
	public static void main(String arg[]){
		int n = 10000;
		int m = 5000;
		Boosting.trainData = new int[n][Vars.AttrNum];
		Boosting.weight = new double[n];

		Random random = new Random(1);
		for (int i=0; i<n; ++i){
			int k=0;
			for (int j=0; j<Vars.AttrNum-1; ++j){
				Boosting.trainData[i][j] = random.nextInt(1000);
				k += Boosting.trainData[i][j];
			}
			if (k > Vars.AttrNum*500) Boosting.trainData[i][Vars.AttrNum-1] = 0;
			else Boosting.trainData[i][Vars.AttrNum-1] = 1;
			Boosting.weight[i] = 1;
		}
		ArrayList<Integer> trainList = new ArrayList<Integer>();
		for (int i=0; i<m; ++i)
			trainList.add(i);
 		RandomForest root = new RandomForest(111, trainList, 0);
		int k = 0;
		for (int i=m; i<n; ++i)
//		for (int i=0; i<m; ++i)
			if (root.predicate(Boosting.trainData[i]) != Boosting.trainData[i][Vars.AttrNum-1])
				++k;
		System.out.println(k);
	}
}
