package di.tools;

import com.hp.hpl.jena.reasoner.rulesys.builtins.Min;

public class NumTools {
	public static double eps = 5e-3;
	
	public static double abs(double x){
		if (x < 0)
			return -x;
		return x;
	}
	
	public static double min(double a, double b){
		if (a < b)
			return a;
		return b;
	}
	
	public static int cmp(double a, double b){
		if (a == 0 || b == 0){
			if (a > b)
				return -1;
			if (a < b)
				return 1;
			return 0;
		}
		if (abs(a-b) < eps || abs((a-b)/min(abs(a), abs(b))) < eps)
			return 0;
		if (a > b)
			return 1;
		return -1;
	}
}
