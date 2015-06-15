package di.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.regex.Pattern;

import di.preprocess.TripleToJson;

public class Test {
	public static void next(String s){
		s = s.substring(1);
	}
	
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
		while(i<s.length() && TripleToJson.doublePrefixPattern.matcher(t + s.charAt(i)).matches())
			t = t + s.charAt(i++);
		ret[0] = t;
		ret[1] = s.substring(i);
		return ret;
	}
	
	static int editDis(String a, String b){
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
		return f[a.length()][b.length()];
	}
	
	public static void main(String[] args){
		System.out.println(editDis("bbbb", "bbbb"));
//		char a = (char)31000;
//		char b = (char)10000;
//		StringBuffer b1 = new StringBuffer();
//		b1.append(a);
//		StringBuffer b2 = new StringBuffer();
//		b2.append(b);
//		System.out.println((int)b1.toString().charAt(0));
//		System.out.println(b1.toString().equals(b2.toString()));
		//		for (int j=0; j<100; ++j){
//			String s = "f";
//			for (int i=0; i<1000; ++i)
//				s = s + "ffdsfd";
//			System.out.println(s.length());
//		}

//		String s = "fds,";
//		String[] q = s.split(",");
//		System.out.println(q.length);
//		System.out.println(q[0]);
		
//		String s = "";
//		double d = Double.parseDouble(s);
//		System.out.println(d);
		
//		Pattern pattern = Pattern.compile("[0-9],[0-9]");
//		System.out.println(pattern.matcher("1,1"));
//		for (String s="fds 6,881(1 ja 2011), fds 1, 2007, fds."; s.length()>0; ){
//			String[] tokens = nextToken(s);
//			s = tokens[1];
//			System.out.println(tokens[0]);
//		}

//		ArrayList<Integer> a = new ArrayList<Integer>();
//		ArrayList<Integer> b = new ArrayList<Integer>();
//		HashSet<Integer> c = new HashSet<Integer>();
//		a.add(3);
//		a.add(2);
//		b.add(3);
//		b.add(4);
//		c.addAll(a);
//		c.addAll(b);
//		for (int i:c)
//			System.out.println(i);
		
//		ArrayList<Integer> list = new ArrayList<Integer>();
//		list.add(1);
//		list.add(4);
//		list.add(5);
//		TreeSet<Integer> set = new TreeSet<Integer>();
//		set.lower(1);		
	}		
}
