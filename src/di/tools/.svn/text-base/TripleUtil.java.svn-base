package di.tools;

import java.util.regex.Pattern;

public class TripleUtil {
	public static String neededLiteral(String s){
		String t = s.replace("\\\\", "__").replace("\\\"", "__");
		if (t.indexOf('"', 1) == t.length()-1)
			return s;
		if (t.charAt(t.indexOf('"', 1)+1) == '@')
			if (t.endsWith("en"))
				return s.substring(0, t.indexOf('"', 1)+1);
			else return null;
		return s.substring(0, t.indexOf('"', 1)+1);
	}
	
	public static String textLiteral(String s){
		String t = s.replace("\\\\", "__").replace("\\\"", "__");
		return s.substring(1, t.indexOf('"', 1));
	}
	
	public static String[] parseTriple(String text){
		int end = text.length()-1;
		while(end >=0 && (text.charAt(end) == ' ' ||
				text.charAt(end) == '\t' || text.charAt(end) == '.'))
				--end;
		String quadruple = text.substring(0, end+1);
		String t = new String(quadruple.toCharArray());
		t = t.replace("\\\\", "__");
		t = t.replace("\\\"", "__");
		char[] tmp = t.toCharArray();
		int k = 0;
		for (int i=0; i<t.length(); ++i)
			if (t.charAt(i) == '"')
				if (k>0) k = 0;
				else k = 1;
			else if (t.charAt(i) == ' ' || t.charAt(i) == '\t')
				if (k == 1)
					tmp[i] = '_';
		t = new String(tmp);
		Pattern pattern = Pattern.compile("[ \t]+");
		String[] split = pattern.split(t);
		String[] ret = new String[4];
		
		int start = 0;
		for (int i=0; i<split.length; ++i){
			start = t.indexOf(split[i].charAt(0), start);
			String s = quadruple.substring(start, start+split[i].length());
			start = start + split[i].length();
			ret[i] = s;
		}
		return ret;
	}
	
	public static void main(String arg[]){
		System.out.println(neededLiteral("\"China\"@en"));
	}
	
	public static String getLabel(String s){
		int l = s.length()-1;
		while(s.charAt(l)!='"'){
			--l;
			if (l<0)
				return null;
		}
		return TextTools.UnicodeToString(s.substring(1, l));
	}
	
	public static String getURI(String s){
		return s.substring(1, s.length()-1);
	}
	
	public static String getFloat(String s){
		if (!s.contains("^^"))
			return null;
		return s.substring(1, s.indexOf("^^")-1);
	}
	
//	public static String DBPEncode(String s){
//		if (!s.startsWith("<"))
//			return s;
//		return s.replaceAll("&", "%26").replaceAll(",", "%2C");
//	}
}
