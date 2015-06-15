package com.sj.freebase.data.rdf;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import di.tools.Vars;

import org.apache.tools.bzip2.CBZip2InputStream;

import org.omg.CORBA.portable.ValueBase;

public class FreebaseRdfizer implements ExtDataTransformer<List<StringBuffer>>{
	private CharSequence fieldSeparator = "\t";
	private String freebaseNsPrefix = "http://rdf.freebase.com/ns/";
	private List<String> skipPredicateRegexList = new ArrayList<String>();
	private List<String> keyRegexList = new ArrayList<String>();
	
	private static final String FB_NAMESPACE = "<http://rdf.freebase.com/ns/type.key.namespace>";
	private static final String FB_VALUE = "<http://rdf.freebase.com/ns/type.value.value>";
	private static final String DEFAULT_SKIP_PREDICATE_REGEX = "user";
	
	private static final String DEFAULT_LANG_REGEX = "/lang/";
	private static final String KEY_TYPE1_REGEX = "type.key.namespace";
	private static final String KEY_TYPE2_REGEX = "type.object.key";
	
	public FreebaseRdfizer () {
		this (null, null, null, null);
	}
	
	public FreebaseRdfizer (CharSequence fieldSeparator, String prefix, List<String> predicatesToSkip, List<String> keyPredicateRegexList) {
		if (fieldSeparator != null)
			this.fieldSeparator = fieldSeparator;
		
		if (prefix != null)
			freebaseNsPrefix = prefix;
		
		if (predicatesToSkip == null) {
			skipPredicateRegexList.add(DEFAULT_SKIP_PREDICATE_REGEX);
		} else {
			skipPredicateRegexList.addAll(predicatesToSkip);
		}
		
		if (keyPredicateRegexList == null) {
			keyRegexList.add(KEY_TYPE1_REGEX);
			keyRegexList.add(KEY_TYPE2_REGEX);
		} else {
			keyRegexList.addAll(keyPredicateRegexList);
		}
	}
	
	private String convertId (String id) throws NullPointerException {
		if (id == null)
			throw new NullPointerException();
		
		return id.replace("/", ".");
	}
	
	// Should linear search to be replaced by something better?
	private boolean skipAssertion (String predicate) {
		for (String regex: skipPredicateRegexList) {
			if (regex.contains(predicate))
				return true;
		}
		
		return false;
	}
	
	private boolean isItKeyTypeAssertion (String predicate) {
		
		/*for (String regex: keyRegexList) {
			if (regex.contains(predicate))
				return true;
		}*/
		
		if (KEY_TYPE1_REGEX.equals(predicate) || KEY_TYPE2_REGEX.equals(predicate))
			return true;
		
		return false;
	}
	
	private List<StringBuffer> processQuadAssertion (String subject, String predicate, String to, String val) throws MalFormedAssertionException {
		List<StringBuffer> triples = new ArrayList<StringBuffer>();;
		StringBuffer triple = new StringBuffer();

		val = val.replace("\\", "\\\\");
		val = val.replace("\"", "\\\"");
		//val = val.replace("`", "\\`");
		triple.append("<");
		triple.append(freebaseNsPrefix);
		triple.append(subject);
		triple.append(">\t<");
		triple.append(freebaseNsPrefix);
		triple.append(predicate);
		triple.append(">\t");
		if (to.length() == 0) {
			triple.append("\"");
			triple.append(val);
			triple.append("\" .");
			triples.add(triple);
		} else if (isItKeyTypeAssertion (predicate)){
			String blankNodeId = subject.replaceAll("[^a-zA-Z0-9]", "_") + to.replaceAll("[^a-zA-Z0-9]", "_") + 
				"_" + val.replaceAll("[ \\t\\n\\x0B\\f\\r]", "_");
			// Create blanknode
			to = convertId(to.substring(1, to.length()));//to.substring(1, to.length()).replace("/", ".");
			triple.append("_:blank");
			triple.append(blankNodeId);
			//triple.append(to.replace(".", "_"));
			triple.append(" .");
			triples.add (triple);
			triple = null;
			triple = new StringBuffer();
			triple.append("_:blank");
			triple.append(blankNodeId);
			//triple.append(to.replace(".", "_"));
			triple.append("\t");
			triple.append(FB_NAMESPACE);
			triple.append("\t<");
			triple.append(freebaseNsPrefix);
			triple.append(to);
			triple.append("> .");
			triples.add (triple);
			triple = null;
			triple = new StringBuffer();
			triple.append("_:blank");
			triple.append(blankNodeId);
			//triple.append(to.replace(".", "_"));
			triple.append("\t");
			triple.append(FB_VALUE);
			triple.append("\t");
			triple.append("\"");
			triple.append(val);
			triple.append("\" .");
			triples.add (triple);			
	
		} else if (to.contains(DEFAULT_LANG_REGEX)) {
			to = to.replace(DEFAULT_LANG_REGEX, "");
			triple.append("\"");
			triple.append(val);
			triple.append("\"@");
			triple.append (to); //lang
			triple.append(" .");
			triples.add(triple);
		} else {
			throw new MalFormedAssertionException(subject + "\t" + predicate + "\t" + to + "\t" + val);
		}
		
		return triples;
	}
	
	

	@Override
	public List<StringBuffer> transformData(String assertion)
			throws NullPointerException, MalFormedAssertionException, SkippedAssertionException, UnsupportedEncodingException {
		if (assertion == null)
			throw new NullPointerException();
		
		String [] splits = assertion.split(fieldSeparator.toString());
		if (splits.length < 3 || splits.length > 4){
			throw new MalFormedAssertionException(assertion);
//			System.out.println(assertion);
//			return null;
		}
		
		List<StringBuffer> triples = null;
		String predicate = convertId (splits [1].substring(1, splits[1].length()));
	
		
		
		if (skipAssertion(predicate))
			throw new SkippedAssertionException (assertion);
		
		if (splits.length == 3) {
			StringBuffer transformedAssertion = null;
			transformedAssertion = new StringBuffer();
			transformedAssertion.append("<");
			transformedAssertion.append(freebaseNsPrefix);
			transformedAssertion.append (convertId (splits [0].substring(1, splits[0].length())));
			transformedAssertion.append(">\t<");
			transformedAssertion.append(freebaseNsPrefix);
			transformedAssertion.append(predicate);
			transformedAssertion.append(">\t<");
			transformedAssertion.append(freebaseNsPrefix);
			transformedAssertion.append(convertId (splits [2].substring(1, splits[2].length())));
			transformedAssertion.append("> . ");	
			triples = new ArrayList<StringBuffer>();
			triples.add(transformedAssertion);
		} else if (splits.length == 4) {
			triples = processQuadAssertion (convertId (splits [0].substring(1, splits[0].length())), 
									predicate, 
									new String (splits [2].getBytes("utf8"), "utf8"), 
									new String (splits [3].getBytes("utf8"), "utf8"));
		}
		
		return triples;
	}
	
	private static void display (List<StringBuffer> assertions) {
		for (StringBuffer assertion: assertions)
			System.out.println (assertion);
	}
	
	private static OutputStreamWriter getWriter(int n) throws UnsupportedEncodingException, FileNotFoundException{
		FileOutputStream zipout = new FileOutputStream( Vars.poseidon + "raw data/Freebase/FreebaseTriples/part-" +n + ".txt" );
		OutputStreamWriter writer = new OutputStreamWriter(zipout, "UTF-8");
		return writer;
	}
	
	public static void main (String [] args) throws Exception {
		FileInputStream fin = new FileInputStream( Vars.poseidon + "raw data/Freebase/freebase-datadump-quadruples.tsv.bz2");
		fin.read();
		fin.read();
		CBZip2InputStream in = new CBZip2InputStream(fin);
		byte[] buffer = new byte [10000];
		String s = "";
		FreebaseRdfizer rdfizer = new FreebaseRdfizer();
		
//		ZipOutputStream zipout = new ZipOutputStream(
//				new FileOutputStream(GlobalVars.poseidon+"freebase-quadruples.zip"));
//		zipout.putNextEntry( new ZipEntry( "freebase-quadruples.txt" ) );
		int len;
		int count = 0;
		int eCount = 0;
		int n = 0;
		OutputStreamWriter writer = getWriter(n);
		while((len = in.read(buffer)) > 0)
			for (int i=0; i<len; ++i)
				if (buffer[i] == '\n'){
					if (++count == 5000000){
						count = 0;
						writer.close();
						writer = getWriter(++n);
					}
					try{
						List<StringBuffer> list = rdfizer.transformData(s);
						for (StringBuffer assertion : list)
							writer.write(assertion + "\n");
					}
					catch (Exception e) {
						System.out.println(++eCount);
						e.printStackTrace();
					}
					s = "";
				}
				else s = s + (char)buffer[i];
		writer.close();
	}
}
