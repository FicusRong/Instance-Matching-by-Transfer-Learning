package com.sj.freebase.data.rdf;

import java.lang.reflect.MalformedParameterizedTypeException;

public class MalFormedAssertionException extends Exception {
	private String assertion = "";
	public MalFormedAssertionException (String assertion) {
		super (assertion);
		
		if (assertion != null)
			this.assertion = assertion;
	}
	
	public MalFormedAssertionException (String assertion, String message) {
		super (message);
		
		if (assertion != null)
			this.assertion = assertion;		
	}
	
	public String getAssertion () {
		return assertion;
	}
}
