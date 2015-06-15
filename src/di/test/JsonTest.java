package di.test;

import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonTest {
	public static void main(String[] args) throws JSONException{
//		JSONObject attr = new JSONObject();
//		attr.put( "skos:prefLabel", "Baden-Baden (Germany)" );
//		attr.put( "redirectFrom", "ZZZ" );
//		
//		JSONArray array = new JSONArray();
//		array.put(1);
//		array.put("a");
//		
//		JSONObject main = new JSONObject();
//		main.put( "uri", "http://example.org/" );
//		main.put( "attr", attr );
//		main.put("array", array);
//		
//		String text = main.toString();
//		JSONObject main2 = new JSONObject( text );
//		
//		System.out.println( main2 );
//		JSONArray a = new JSONArray();
//		a.put(1);
//		a.put(2);
//		JSONArray b = new JSONArray();
//		b.put(3);
//		b.put(4);
//		JSONArray c = new JSONArray();
//		for (int i=0; i<c.length(); ++i)
//			System.out.println(c.get(i));
		
		JSONObject json = new JSONObject();
		json.put("a", 1);
		json.put("b", 2);
		for (Iterator<String> i = json.keys(); i.hasNext();){
			String t = i.next();
			System.out.println(t);
			System.out.println(json.getInt(t));
		}
	}
}
