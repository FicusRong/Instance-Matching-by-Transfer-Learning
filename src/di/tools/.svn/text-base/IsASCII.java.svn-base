package di.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

public class IsASCII {

	private String str = null;
	BufferedReader br = null;
	String tableLocation = Vars.poseidon + "Raw Data/references/specialchartable.txt";
	HashMap<String, String> hm = new HashMap<String, String>();
	public IsASCII(){
		initHashMap();
	}
	public void initHashMap(){
		hm.put("í", "i");
		hm.put("ú", "u");
		hm.put("Ü", "U");
		hm.put("ü", "u");
		hm.put("ó", "o");
		hm.put("ŏ", "o");
		hm.put("á", "a");
		hm.put("ö", "o");
		hm.put("é", "e");
		hm.put("ī", "i");
		hm.put("ā", "a");
		hm.put("č", "c");
	}
	public IsASCII(String str){
		this.str = str;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(tableLocation), "UTF-8"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		String myreadline = null;
		String[] splitline;
		try {
			while((myreadline=br.readLine())!=null){
				splitline = myreadline.split("\t");
				hm.put(splitline[0], splitline[1]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public boolean judge(String string){
		this.str = string;
		for(int i=0;i<this.str.length();i++){
			if((int)str.charAt(i)>127){
				return false;
			}
		}
		return true;
	}
	public String shift(String string) throws IOException{
		this.str = string;
		char tmpchar;
		char replacechar;
		String replacestr = str;
		for(int i=0;i<this.str.length();i++){
			if((int)(tmpchar = replacestr.charAt(i)) > 127){
				if(hm.containsKey(Character.toString(tmpchar))){
					String tmpstr = (String) hm.get(Character.toString(tmpchar));
					replacechar = tmpstr.charAt(0);
//					System.out.println(i+" >>> "+tmpchar+" >>> "+replacechar);
					//shift char to string
					replacestr = replacestr.replace(tmpchar, replacechar);
					//System.out.println( replacestr );
				}else{
					return string;
				}
			}
		}
		return replacestr;
	}
	public static void main(String[] args) throws IOException {
		IsASCII isa = new IsASCII();
		isa.initHashMap();
		
		String teststr = "hello";
		System.out.println(isa.judge(teststr));
		System.out.println(isa.shift(teststr));
		
		teststr = "Wazīristān";
		System.out.println(isa.judge(teststr));
		System.out.println(isa.shift(teststr));
		
		teststr = "Sheykh Khomāţ";
		System.out.println(isa.judge(teststr));
		System.out.println(isa.shift(teststr));
		
		teststr = "Pucón";
		System.out.println(isa.judge(teststr));
		String name = isa.shift(teststr);
		System.out.println(name);
	}
	public static boolean test(String string){
		for(int i=0;i<string.length();i++){
			if((int)string.charAt(i)>127){
				return false;
			}
		}
		return true;
	}
}
