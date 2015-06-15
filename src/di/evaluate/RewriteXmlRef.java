package di.evaluate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

import di.tools.Vars;

public class RewriteXmlRef {
	
	public static void main(String args[]) throws IOException{
		FileReader fileReader = new FileReader("D:/OAEI/DI03062010/" +"dailymed-alignment.xml");
		BufferedReader bufferReader = new BufferedReader(fileReader);
		FileWriter writer = new FileWriter("D:/OAEI/DiRef/" + Vars.DM + "_" + Vars.DBP);
		HashSet<String> set = new HashSet<String>(); 
		while(true){
			String s = bufferReader.readLine();
			if (s == null)
				break;
			try{
				if (s.contains("<entity1")){
					String t = bufferReader.readLine();
					s = "<" + s.substring(s.indexOf("rdf:resource='") + 14, s.indexOf("'/>")) + ">";
					t = "<" + t.substring(t.indexOf("rdf:resource='") + 14, t.indexOf("'/>")) + ">";
					if (t.contains("www.dbpedia")){
						int i = t.indexOf("www.dbpedia");
						t = t.substring(0, i) + t.substring(i + 4);
					}						
					if (t.startsWith(Vars.uriPrefix(Vars.DBP))){
						String output = s + " _c_" + t + "\n";
						if (!set.contains(output)){
							set.add(output);
							writer.write(output);
						}
					}
				}
			}
			catch (Exception e) {
				System.out.println(s);
			}
		}
		writer.close();
	}

}
