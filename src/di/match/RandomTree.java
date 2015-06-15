package di.match;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import di.tools.Vars;

public class RandomTree {
	public RandomTree left = null;
	public RandomTree right = null;
	public int label, attr, theta;
	int pos, neg;
	
	public RandomTree(ArrayList<Integer> list, int randomSeed){
		pos = neg = 0;
		label = -1;
		for (int i=0; i<list.size(); ++i)
			if (Boosting.trainData[list.get(i)][Vars.AttrNum-1] == 0) neg++;
			else pos++;
//		if (list.size() == Tradaboost.m)
//			System.out.println(pos +"  "+neg);
		if (pos == 0){
			label = 0;
			return;
		}
		if (neg == 0){
			label = 1;
			return;
		}
		
		double maxG = -10;
		double orgE = entropy(pos, neg);
		Random random = new Random(randomSeed);
		int tmp = Vars.attributeK;
		if (Vars.attributeK == 0)
			tmp = Vars.AttrNum - 1;
		for (int T=0; T<tmp; ++T){
			int i = random.nextInt(Vars.AttrNum - 1);
			if (Vars.attributeK == 0)
				i = T;
			for (int j=0; j<Vars.ignoredAttr.length; ++j)
				if (i == Vars.ignoredAttr[j]-1){
					i = -1;
					break;
				}
			if (i<0) continue;
			ArrayList<IntPair> tmpList = new ArrayList<IntPair>();
			for (int j=0; j<list.size(); ++j)
				tmpList.add(new IntPair(Boosting.trainData[list.get(j)][i], list.get(j)));
			Collections.sort(tmpList);
			int pos0 = 0;
			int neg0 = 0;
			for (int j=0; j<tmpList.size(); ++j){
				if (j > 0 && tmpList.get(j).v > tmpList.get(j-1).v){
					double E = entropy(pos0, neg0) * (pos0 + neg0) / total();
					E += entropy(pos-pos0, neg-neg0) * (total() - pos0 - neg0) / total();
//					System.out.println(E +" "+ orgE);
//					double G = (orgE - E) / entropy(pos0 + neg0, total() - pos0 - neg0);
					double G = orgE - E;
					if (G > maxG){
						maxG = G;
						attr = i;
						theta = (tmpList.get(j).v + tmpList.get(j-1).v + 1)/2;
					}
				}
//				System.out.println(maxG);
				int k = tmpList.get(j).no; 
				if (Boosting.trainData[k][Vars.AttrNum-1] == 0) neg0++;
				else pos0++;
			}
		}
//		System.out.println(maxG);
		if (maxG < 0){
			if (pos > neg) label = 1;
			else label = 0;
//			System.out.println(pos +" " + neg + " " + maxG);
			return;
		}
		
		ArrayList<Integer> leftList = new ArrayList<Integer>();
		ArrayList<Integer> rightList = new ArrayList<Integer>();
		for (int i=0; i<list.size(); ++i)
			if (Boosting.trainData[list.get(i)][attr] < theta)
				leftList.add(list.get(i));
			else rightList.add(list.get(i)); 
		left = new RandomTree(leftList, randomSeed);
		right = new RandomTree(rightList, randomSeed);
	}
	
	public int predicate(int[] q){
		if (label != -1)
			return label;
		if (q[attr] < theta)
			return left.predicate(q);
		else return right.predicate(q);
	}
	
	public double entropy(double x, double y){
		if (x<Vars.eps || y<Vars.eps)
			return 0;
		double s = x + y;
		double ret = - x/s * Math.log(x/s);
		ret -= y/s * Math.log(y/s);
		return ret;
	}
	
	public int total(){
		return neg + pos;
	}
	
	public void print(String head){
		if (label != -1)
			System.out.println(head + label);
		else{
			System.out.println(head + attr + " " + theta +" {");
			left.print(head+"\t");
			System.out.println(head + "-----------");
			right.print(head+"\t");
			System.out.println("}");
		}
	}
	
	class IntPair implements Comparable<IntPair>{
		public int v, no;
		
		public IntPair(int v, int no){
			this.v = v;
			this.no = no;
		}
		
		@Override
		public int compareTo(IntPair o) {
			return v - o.v;
		}			
	}
	
	public static void main(String arg[]){
		int n = 10000;
		int m = 5000;
		int seed = 3;
		Boosting.trainData = new int[n][Vars.AttrNum];
		Boosting.weight = new double[n];
		Random random = new Random(seed);
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
		ArrayList<Integer> list = new ArrayList<Integer>();
		for (int i=0; i<m; ++i)
			list.add(i);
		RandomTree root = new RandomTree(list, seed);
		int k = 0;
		for (int i=m; i<n; ++i)
//		for (int i=0; i<m; ++i)
			if (root.predicate(Boosting.trainData[i]) != Boosting.trainData[i][Vars.AttrNum-1])
				++k;
		System.out.println("err: " + k);
//		}
	}
}
