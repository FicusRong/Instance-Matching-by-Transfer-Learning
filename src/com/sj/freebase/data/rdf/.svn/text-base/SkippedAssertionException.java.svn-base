package com.sj.freebase.data.rdf;

public class SkippedAssertionException extends Exception {
	private String assertion = "";
	public SkippedAssertionException (String assertion) {
		super (assertion);
		
		if (assertion != null)
			this.assertion = assertion;
	}
	
	public SkippedAssertionException (String assertion, String message) {
		super (message);
		
		if (assertion != null)
			this.assertion = assertion;		
	}
	
	public String getAssertion () {
		return assertion;
	}
}
