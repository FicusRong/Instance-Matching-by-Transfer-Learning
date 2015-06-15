package di.match;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import di.tools.Vars;

public class C45Tree {
	public C45Tree left = null;
	public C45Tree right = null;
	public int label, attr, theta;
	double pos, neg, error;
	
	public C45Tree(ArrayList<Integer> list){
//		System.out.println("Building-tree size: " + list.size());
		pos = neg = 0;
		label = -1;
		for (int i=0; i<list.size(); ++i)
			if (Tradaboost.trainData[list.get(i)][Vars.AttrNum-1] == 0) neg += Tradaboost.weight[list.get(i)];
			else pos += Tradaboost.weight[list.get(i)];
		if (pos < Vars.eps){
			label = 0;
			error = C45Error.get(0, neg);
			return;
		}
		if (neg < Vars.eps){
			label = 1;
			error = C45Error.get(0, pos);
			return;
		}
		
		//divide
		double minE = -1;
		for (int i=0; i<Vars.AttrNum-1; ++i){
			ArrayList<IntPair> tmpList = new ArrayList<IntPair>();
			for (int j=0; j<list.size(); ++j)
				tmpList.add(new IntPair(Tradaboost.trainData[list.get(j)][i], list.get(j)));
			Collections.sort(tmpList);
			double pos0 = 0;
			double neg0 = 0;
			for (int j=0; j<tmpList.size(); ++j){
				if (j>0 && tmpList.get(j).v > tmpList.get(j-1).v){
					double E = entropy(pos0, neg0)*(pos0 + neg0);
					E += entropy(pos-pos0, neg-neg0)*(total() - pos0 - neg0);
//					E /= entropy(pos0 + neg0, total() - pos0 - neg0);
					if (minE < 0 || E < minE){
						minE = E;
						attr = i;
						theta = (tmpList.get(j).v + tmpList.get(j-1).v + 1) / 2;
					}
				}
				int k = tmpList.get(j).no; 
				if (Tradaboost.trainData[k][Vars.AttrNum-1] == 0) neg0 += Tradaboost.weight[k];
				else pos0 += Tradaboost.weight[k];
			}
		}
		
		if (minE < 0){
			if (pos > neg){
				label = 1;
				error = C45Error.get(neg, total());
			}
			else{
				label = 0;
				error = C45Error.get(pos, total());
			}
			return;
		}
		
//		System.out.println(t1+" "+t2+" "+t3+" "+t4+" .");
		ArrayList<Integer> leftList = new ArrayList<Integer>();
		ArrayList<Integer> rightList = new ArrayList<Integer>();
		for (int i=0; i<list.size(); ++i)
			if (Tradaboost.trainData[list.get(i)][attr] < theta)
				leftList.add(list.get(i));
			else rightList.add(list.get(i)); 
		left = new C45Tree(leftList);
		right = new C45Tree(rightList);
		error = left.error + right.error;
		
		//prune
		double e2, e3;
		if (pos > neg) e2 = C45Error.get(neg, total());
		else e2 = C45Error.get(pos, total());
		
		double tmpError = 0;
		if (left.total() > right.total())
			tmpError = left.trainError(list);
		else tmpError = right.trainError(list);
		e3 = C45Error.get(tmpError, total());
//		System.out.println(error +"  "+e2 +"  "+e3);
		if (e2 < error+0.1 && e2 < e3+0.1){
			if (pos > neg){
				label = 1;
				error = neg;
			}
			else{
				label = 0;
				error = pos;
			}
		}
		else if (e3 < error+0.1){
			error = tmpError;
			if (left.total() > right.total()){
				label = left.label;
				attr = left.attr;
				theta = left.theta;
				right = left.right;
				left = left.left;
			}
			else{
				label = right.label;
				attr = right.attr;
				theta = right.theta;
				left = right.left;
				right = right.right;
			}
		}
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
	
	public double total(){
		return neg + pos;
	}
	
	public double trainError(ArrayList<Integer> list){
		double sum = 0;
		if (label != -1){
			for (int i=0; i<list.size(); ++i)
				if (Tradaboost.trainData[list.get(i)][Vars.AttrNum-1] != label)
					sum += Tradaboost.weight[list.get(i)];
			return sum;
		}
		ArrayList<Integer> leftList = new ArrayList<Integer>();
		ArrayList<Integer> rightList = new ArrayList<Integer>();
		for (int i=0; i<list.size(); ++i)
			if (Tradaboost.trainData[list.get(i)][attr] < theta)
				leftList.add(list.get(i));
			else rightList.add(list.get(i)); 
		return left.trainError(leftList) + right.trainError(rightList);
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
		int m = 100;
		Boosting.trainData = new int[n][Vars.AttrNum];
		Boosting.weight = new double[n];
		Random random = new Random();
		for (int i=0; i<n; ++i){
			int k=0;
			for (int j=0; j<Vars.AttrNum-1; ++j){
				Boosting.trainData[i][j] = random.nextInt(1000);
				k += Boosting.trainData[i][j];
			}
			if (k > 4000) Boosting.trainData[i][Vars.AttrNum-1] = 0;
			else Boosting.trainData[i][Vars.AttrNum-1] = 1;
			Boosting.weight[i] = 1;
		}
		ArrayList<Integer> list = new ArrayList<Integer>();
		for (int i=0; i<m; ++i)
			list.add(i);
		C45Tree root = new C45Tree(list);
		int k = 0;
//		for (int i=Tradaboost.n/2; i<Tradaboost.n; ++i)
		for (int i=m; i<n; ++i)
			if (root.predicate(Boosting.trainData[i]) != Boosting.trainData[i][Vars.AttrNum-1])
				++k;
		System.out.println(k);
//		}
	}
}
