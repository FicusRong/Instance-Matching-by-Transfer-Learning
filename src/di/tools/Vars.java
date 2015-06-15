package di.tools;

import java.util.ArrayList;

public class Vars {
	public static final long prec = 100000;
	public static final double conf = 0.8;
	public static final int AttrNum = 14;
	public static final int randomFrom = 111;
	public static final int randomNum = 3;
	public static final int N = 20;
	public static final double randomForestD = 3;
	public static final boolean useSourceDomain = true;
	public static final boolean transfer = true;
	public static final int pruneRandomTreeN = 33;
	public static final int TradaboostRandomTreeN = 33;
	public static final int TransferboostRandomTreeN = 33;
	public static final int attributeK = 0;
	public static final double eps = 1e-8;
	public static final int trainingRate = 2;
	public static final int MaxConfStringSize = 100000;
	public static final int[] ignoredAttr = {};
	
	public static final String FB = "Freebase";
	public static final String DBP = "DBpedia";
	public static final String DM = "DailyMed";
	public static final String SD = "Sider";
	public static final String DB = "DrugBank";
	public static final String DS = "DiseaSome";
	public static final String GN = "GeoNames";
	public static final String LGD = "LinkedGeoData";
	
	public static final String TestOutput = "/Users/rongshu/di/Test";
	public static final String Rawdata = "/Users/rongshu/di/rawdata/";
	public static final String ExtraLabel = "/Users/rongshu/di/ExtraLabel/";
	public static final String PropertyText = "/Users/rongshu/di/PropertyText/";
	public static final String LiteralObject = "/Users/rongshu/di/LiteralObject/";
	public static final String MergedBN = "/Users/rongshu/di/MergedBN/";
	public static final String Json = "/Users/rongshu/di/Json/";
	public static final String WordCount = "/Users/rongshu/di/WordCount/";
	public static final String TfIdf = "/Users/rongshu/di/TfIdf/"; 
	public static final String PreMatch = "/Users/rongshu/di/PreMatch/";
	public static final String CorrectAlignment = "/Users/rongshu/di/CorrectAlignment/";
	public static final String IdPreMatch = "/Users/rongshu/di/IdPreMatch/";
	public static final String DisCalc = "/Users/rongshu/di/DisCalc/";
	public static final String StandardOutput = "/Users/rongshu/di/StandardOutput/";
	public static final String PruneStdOutput = "/Users/rongshu/di/PruneStdOutput/";
	public static final String NearbyVector = "/Users/rongshu/di/NearbyVector/";
	public static final String NearbyUnlabeledVector = "/Users/rongshu/di/NearbyUnlabeledVector/";
	public static final String IdfVector = "/Users/rongshu/di/IdfVector/";
	public static final String Tradaboost = "/Users/rongshu/di/Tradaboost/";
	
	public static final String poseidonWin = "//poseidon/Share/Personal/rongshu/DI@OAEI2010";
	public static final String poseidon = "/mnt/poseidon/Personal/rongshu/DI@OAEI2010";
	
	public static final String wordList = "wordList";
	public static final String wordCount = "wordCount";
	public static final String dataSet = "dataSet";
	public static final String docNum = "docNum";
	public static final String randomSeed = "reandomSeed";
	public static String labelProperty = "<rdf:label>";
	
	public static final String uri = "uri";
	public static final String label = "label";
	public static final String link = "link";
	public static final String num = "num";
	public static final String date = "date";
	public static final String textLong = "textLong";
	public static final String textMiddle = "textMiddle";
	public static final String textShort = "textShort";
	public static final String property = "property";
	public static final String labelString = "labelString";
	
	public static ArrayList<String> sourceDomains;
	
	public static boolean prune(String dataSet){
		if (dataSet.equals(SD + "_" + DBP))
			return true;
		if (dataSet.equals(DM + "_" + DBP))
			return true;
		return false;
	}
	
	public static int prunePercent(String dataSet){
		if (dataSet.equals(SD + "_" + DBP))
			return 20;
		return 20;
	}
	
	public static int preMatchTopWords(String dataSet){
		if (dataSet.equals(SD + "_" + DS))
			return 3;
		if (dataSet.equals(LGD + "_" + DBP))
			return 0;
		if (dataSet.equals(DBP + "_" + GN))
			return 0;
		return 10;
	}
	
	public static int preMatchM(String dataSet){
		if (dataSet.equals(SD + "_" + DS))
			return 70;
		if (dataSet.equals(SD + "_" + DB))
			return 100;
		if (dataSet.equals(LGD + "_" + DBP))
			return 30;
		if (dataSet.equals(DBP + "_" + GN))
			return 30;
		return 1000;
	}
	
	public static int sourceRate(String dataSet){
		if (dataSet.equals(SD + "_" + DS))
			return 5;
		return 10;
	}
	
	public static String combDataSets(String q[]){
		String s = "";
		for (int i=0; i<q.length; ++i){
			if (i>0) s = s + '_';
			s = s + q[i];
		}
		return s;
	}
	
	public static String uriPrefix(String dataSet){
		if (dataSet.equals(DBP))
			return "<http://dbpedia.org";
		if (dataSet.equals(DB))
			return "<http://www4.wiwiss.fu-berlin.de/drugbank";
		if (dataSet.equals(DM))
			return "<http://www4.wiwiss.fu-berlin.de/dailymed";
		if (dataSet.equals(SD))
			return "<http://www4.wiwiss.fu-berlin.de/sider";
		if (dataSet.equals(DS))
			return "<http://www4.wiwiss.fu-berlin.de/diseasome";
		if (dataSet.equals(LGD))
			return "<http://linkedgeodata.org";
		return "<http://rdf.freebase.com";
	}
	
	public static int docNum(String dataSet){
		if (dataSet.equals(DBP + "_" + FB))
			return 64410703;
		if (dataSet.equals(SD + "_" + DS))
			return 10801;
		if (dataSet.equals(SD + "_" + DB))
			return 22239;
		if (dataSet.equals(SD + "_" + DM))
			return 12643;
		if (dataSet.equals(SD + "_" + DBP))
			return 16773106;
		if (dataSet.equals(LGD + "_" +DBP))
			return 23020504;
		if (dataSet.equals(DBP + "_" + GN))
			return 49833146;
		return 0;
	}
}
