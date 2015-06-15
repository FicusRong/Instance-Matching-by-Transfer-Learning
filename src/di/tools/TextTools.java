package di.tools;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextTools
{
	/**
	 * Encode URL
	 * @param str
	 * 			the URL that to be encoded
	 * @return
	 * 			encoded URL
	 */
	public static String encoder( String str )
	{
		try
		{
			return URLEncoder.encode( str, "UTF-8" );
		}
		catch( UnsupportedEncodingException e )
		{
			throw new RuntimeException( "Broken VM does not support UTF-8" );
		}
	}
	
	/**
	 * Decode URL
	 * @param str
	 * 			the URL that to be decoded
	 * @return
	 * 			decoded URL
	 */
	public static String decoder( String str )
	{
		try
		{
			return URLDecoder.decode( str, "UTF-8" );
		}
		catch( UnsupportedEncodingException e )
		{
			throw new RuntimeException( "Broken VM does not support UTF-8" );
		}
		catch( Exception e )
		{
			return "";
		}
	}
	
	/**
	 * Convert Unicode expression to the original string.
	 * For N-Triples decoding (http://www.w3.org/TR/rdf-testcases/#ntrip_strings).
	 * @param str
	 * 			the string that to be decoded
	 * @return
	 * 			decoded string
	 */
	public static String UnicodeToString( String str )
	{
		Pattern pattern = Pattern.compile( "(\\\\u(\\p{XDigit}{4}))|(\\\\U(\\p{XDigit}{8}))" );
		Matcher matcher = pattern.matcher( str );
		char ch;
		while( matcher.find() )
		{
			ch = (char) Integer.parseInt( matcher.group( 0 ).substring( 2 ), 16 );
			str = str.replace( matcher.group( 0 ), ch + "" );
		}
		str = str.replace( "\\t", "\t" );
		str = str.replace( "\\n", "\n" );
		str = str.replace( "\\r", "\r" );
		str = str.replace( "\\\"", "\"" );
		str = str.replace( "\\\\", "\\" );
		return str;
	}
	
	public static double similarity( String str1, String str2 )
	{
		if( str1.equalsIgnoreCase( str2 ) )
			return 1.0;
			
		int length_max = Math.max( str1.length(), str2.length() );
		int length_min = Math.min( str1.length(), str2.length() );
		return 1-LevenshteinDistance( str1, str2 )*1.0/length_max;
	}
	
	public static double similarity_cns( String str1, String str2 )
	{
		if( str1.equalsIgnoreCase( str2 ) )
			return 1.0;
		
		str1 = str1.toLowerCase();
		str2 = str2.toLowerCase();
			
		int length_max = Math.max( str1.length(), str2.length() );
		
		double sim = 1-LevenshteinDistance( str1, str2 )*1.0/length_max;
		
		if( str1.contains( str2 ) )
			sim = sim + (1-sim)*0.5;

		return sim;
	}
	
	public static int LevenshteinDistance( String str1, String str2 )
	{
		int m = str1.length();
		int n = str2.length();
		int[][] matrix = new int[m+1][n+1];
		              
		for( int i = 0; i <= m; ++i )
			matrix[i][0] = i;
		for( int j = 0; j <= n; ++j )
			matrix[0][j] = j;

		for( int j = 1; j <= n; ++j )
		{
			for( int i = 1; i <= m; ++i )
			{
				if( str1.charAt( i-1 ) == str2.charAt( j-1 ) )
				{
					matrix[i][j] = matrix[i-1][j-1];
				}
				else
				{
					matrix[i][j] = Math.min( matrix[i-1][j] + 1, matrix[i][j-1] + 1 );
					matrix[i][j] = Math.min( matrix[i-1][j-1] + 1, matrix[i][j] );
				}
			}
		}
		return matrix[m][n];
	}
}
