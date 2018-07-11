package com.kAnonymity_maven;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Serializable;
import scala.Tuple2;

class textUtil2 {

	// load stop word list from file
	public static TreeMap<String, Integer> loadMap(String inStr) {

		TreeMap<String, Integer> outMap = new TreeMap<String, Integer>();
		try {
			FileInputStream stream = new FileInputStream(inStr);
			InputStreamReader reader = new InputStreamReader(stream);
			BufferedReader buffer = new BufferedReader(reader);

			String label = new String("");
			Object oldValue;

			while ((label = buffer.readLine()) != null && !label.equals("")) {

				oldValue = outMap.get(label);

				if (oldValue == null)
					outMap.put(label, new Integer(1));
			}
			stream.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return outMap;
	}

	// load stop word list from file
	public static Map loadIdfMap(String inStr) {

		Map idfMap = new TreeMap();

		try {
			FileInputStream stream = new FileInputStream(inStr);
			InputStreamReader reader = new InputStreamReader(stream);
			BufferedReader buffer = new BufferedReader(reader);

			String label = new String("");

			while ((label = buffer.readLine()) != null) {

				StringTokenizer st = new StringTokenizer(label, "|");

				String kStr = st.nextToken();
				String vStr = st.nextToken();

				Double newValue = new Double(vStr);

				idfMap.put(kStr, newValue);

			} // end_of_while

			stream.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return idfMap;
	}

	public static Writer createTXTFile(String inStr) {

		Writer outFile;

		try {
			outFile = new BufferedWriter(new FileWriter(new File(inStr)));
			return outFile;
		} catch (IOException e) {
			System.out.println(e);
		}
		return null;
	}

	public static void saveTXTFile(Writer outFile) {

		try {
			outFile.close();
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	public static void writeString(Writer inWriter, String inStr) {

		try {
			inWriter.write(inStr);
			inWriter.flush();
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	public static void PrintArray(boolean[] inVector, int idx) {
		for (int i = 0; i < idx; ++i)
			System.out.print("[" + i + ":" + inVector[i] + "]");
		System.out.println();
	}

	public static void PrintArray(int[] inVector, int idx) {
		for (int i = 0; i < idx; ++i)
			System.out.print("[" + i + ":" + inVector[i] + "]");
		System.out.println();
	}

	public static void PrintArray(double[] inVector, int idx) {
		for (int i = 0; i < idx; ++i)
			System.out.print(inVector[i] + ":");
		System.out.println();
	}

	public static void PrintArray0(double[][] inVector, int idx) {

		for (int i = 0; i < idx; ++i)
			System.out.print("[" + i + ":" + inVector[0][i] + ":" + inVector[1][i] + "]");
		System.out.println();
	}

}

class Parameter2 {

	public static int KValue = 5;

	public static int dataSize = 100;

	public static final int totalAttrSize = 7;
	public static String joinAttrListStr = "0"; //

	// -------------------------cluster_option----------------------------------
	public static final String inputFileName = new String("data/inputFile.txt");

	public static String resizingInputData_T1 = "data/t1_resizingBy_" + dataSize + ".txt";

	public static final String genTreeFileName = new String("data/gTree.txt");
	public static String resultFileName = "result/" + joinAttrListStr + "_" + dataSize;

	public static final String original_T1 = new String("data/t1_sample.txt");

	public static String k_anonymous_tree_T1 = "data/gTree_T1_";

	public static String k_member_T1 = "data/k_member_T1_";

	public static final String transformed_kmember_T1 = new String("data/k_member_T1_" + KValue + ".txt");
	public static final String transformed_kanonimity_T1 = new String("data/gTree_T1_" + KValue + ".txt");

	public static final String ILmatrix_T1 = new String("data/ILmatrix_T1_" + KValue + ".txt");

	// ------------------------local_option--------------------------------------------------

	// public static final String inputFileName = new String("data/inputFile.txt");
	//
	// public static String resizingInputData_T1 = "data/t1_resizingBy_" + dataSize
	// + ".txt";
	//
	// public static final String genTreeFileName = new String("data/gTree.txt");
	// public static String resultFileName = "result/" + joinAttrListStr + "_" +
	// dataSize;
	//
	// public static final String original_T1 = new String("data/t1_sample.txt");
	//
	// public static String k_anonymous_tree_T1 = "data/gTree_T1_";
	//
	// public static String k_member_T1 = "data/k_member_T1_";
	//
	// public static final String transformed_kmember_T1 = new
	// String("data/k_member_T1_" + KValue + ".txt");
	// public static final String transformed_kanonimity_T1 = new
	// String("data/gTree_T1_" + KValue + ".txt");
	//
	// public static final String ILmatrix_T1 = new String("data/ILmatrix_T1_" +
	// KValue + ".txt");
}

public class kAnonymity_spark_local implements Serializable {
	int count = 0;
	String tranformedStr2 = new String();
	ArrayList<Integer> kts2 = new ArrayList<Integer>();
	// static SparkConf conf = new
	// SparkConf().setMaster("local").setAppName("kAnonymity_spark");
	// static SparkConf conf = new
	// SparkConf().setMaster("yarn-cluster").setAppName("kAnonymity_spark");
	static SparkConf conf = new SparkConf().setMaster("local").setAppName("kAnonymity_spark");
	static JavaSparkContext sc1 = new JavaSparkContext(conf);
	private int KValue = Parameter2.KValue;
	private String genTreeFileName = new String(Parameter2.genTreeFileName);

	private String inputFile_T1 = new String(Parameter2.resizingInputData_T1);

	private HashMap<String, Integer> maxMap = new HashMap<String, Integer>();
	private HashMap<String, ArrayList<Integer>> rangeMap = new HashMap<String, ArrayList<Integer>>();
	private static ArrayList<String> projectionList = new ArrayList<String>();
	private ArrayList<Double> projectionSizeList = new ArrayList<Double>();

	private ArrayList<ArrayList> tupleList_T1 = new ArrayList<ArrayList>();

	private ArrayList<String> transfromed_tupleList_T1 = new ArrayList<String>();
	static JavaPairRDD<String, Integer> line4;

	private int IRcnt = 0;
	private double curIR = 0.0;

	public void loadGenTree() {
		System.out.println("loadGenTree Start!!");
		try {
			FileInputStream stream = new FileInputStream(this.genTreeFileName);
			InputStreamReader reader = new InputStreamReader(stream);
			BufferedReader buffer = new BufferedReader(reader);

			while (true) {
				String label = buffer.readLine();
				if (label == null)
					break;

				StringTokenizer st = new StringTokenizer(label, "|");
				String attrName = st.nextElement().toString();

				for (int i = 0; i < need_att.size(); i++) {
					if (attrName.equals(need_gTree.get(need_att.get(i)))) {
						Integer treeLevel = new Integer(st.nextElement().toString());
						String valueStr = st.nextElement().toString();

						// update min and max
						Integer curMax = this.maxMap.get(attrName);
						if (curMax == null)
							this.maxMap.put(attrName, treeLevel);
						else if (curMax.intValue() < treeLevel.intValue())
							this.maxMap.put(attrName, treeLevel);

						// insert range list
						ArrayList<Integer> tempArr = new ArrayList<Integer>();
						StringTokenizer valueStr_st = new StringTokenizer(valueStr, "_");
						while (valueStr_st.hasMoreTokens()) {
							tempArr.add(new Integer(valueStr_st.nextToken()));
						}

						this.rangeMap.put(attrName + "-" + treeLevel, tempArr);
						break;
					}
				}

			}

			System.out.println("maxMap : " + this.maxMap);
			System.out.println("rangeMap : " + this.rangeMap);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("loadGenTree Finish!!");
		}

	}

	private double exIR = 0.0;
	private ArrayList<Integer> fitNode;

	public kAnonymity_spark_local() {

		if (need_att.size() != 0) {
			for (int i = 0; i < need_att.size(); i++) {
				switch (need_att.get(i)) {
				case 1:
					this.projectionList.add("age");
					break;
				case 2:
					this.projectionList.add("sex");
					break;
				case 3:
					this.projectionList.add("surgery");
					break;
				case 4:
					this.projectionList.add("length");
					break;
				case 5:
					this.projectionList.add("location");
					break;

				}
			}
			this.projectionList.add("disease");
		}

		else {
			this.projectionList.add("age");
			this.projectionList.add("sex");
			this.projectionList.add("surgery");
			this.projectionList.add("length");
			this.projectionList.add("location");
			// this.projectionList.add("a2");
			this.projectionList.add("disease");
		}

		this.projectionSizeList.add(61.0);
		this.projectionSizeList.add(2.0);
		this.projectionSizeList.add(11.0);
		this.projectionSizeList.add(2.0);
		this.projectionSizeList.add(51.0);

		System.out.println(this.inputFile_T1);
	}

	public void loadData(String inputFileName, ArrayList<ArrayList> curTupleList) {
		System.out.println("loadData Start!!");
		try {
			FileInputStream stream = new FileInputStream(inputFileName);
			InputStreamReader reader = new InputStreamReader(stream);
			BufferedReader buffer = new BufferedReader(reader);

			int curCount = 0;
			while (true) {
				String label = buffer.readLine();
				if (label == null)
					break;

				ArrayList curTuple = new ArrayList();
				StringTokenizer st = new StringTokenizer(label, "|");

				int need_att_index = 0;
				int flag = 1;
				int i = 1;
				String temp;
				while (true) {
					if (flag == 0)
						break;
					temp = st.nextToken();

					if (i == need_att.get(need_att_index)) {
						curTuple.add(new Integer(temp));
						need_att_index++;
						i++;
					}

					else {
						i++;
					}

					if (need_att_index == need_att.size())
						flag = 0;
				}

				String disease = null;
				while (st.hasMoreTokens()) {
					disease = st.nextToken();
				}

				curTuple.add(new String(disease)); // disease
				curTupleList.add(curTuple);

			}

			System.out.println("curTupleList : " + curTupleList + "\nlogenData()_FINSH\n");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("loadData Finish!!");
		}
	}

	public boolean performGeneralization(final ArrayList<Integer> curNode, ArrayList<ArrayList> curTupleList,
			final ArrayList<String> transfromed_curTupleList, JavaRDD<ArrayList> info) {

		final int attrNumber = this.projectionList.size();
		double nodeIR = 0.0;
		boolean stopFlag = true;
		HashMap<String, ArrayList<String>> anonymizedResult = new HashMap<String, ArrayList<String>>();
		HashMap<String, Integer> kts1 = new HashMap<String, Integer>();
		

		JavaRDD<ArrayList> line = info.map(new Function<ArrayList, ArrayList>() {
			public ArrayList call(ArrayList x) {

				ArrayList<String> temp = new ArrayList<String>();
				String tranformedStr = new String();
				
				for (int k = 0; k < attrNumber - 1; k++) {
					String attrName = projectionList.get(k);
					int treeLevel = curNode.get(k).intValue();
					int curAttrValue = ((Integer) x.get(k)).intValue();

					if (treeLevel > 0) {
						ArrayList<Integer> curRangeList = rangeMap.get(attrName + "-" + treeLevel);
						if (curRangeList.size() == 2) {
							tranformedStr = tranformedStr + "|" + curRangeList.get(0) + "_" + curRangeList.get(1);
							// nodeIR += 1.0;
						} else {
							for (int m = 0; m < curRangeList.size() - 1; ++m) {
								int curMin = curRangeList.get(m);
								int curMax = curRangeList.get(m + 1);

								if ((curMin <= curAttrValue) && (curAttrValue <= curMax)) {
									tranformedStr = tranformedStr + "|" + curMin + "_" + curMax;
									break;
								}
							}
						}
					}

					else {
						tranformedStr = tranformedStr + "|" + curAttrValue;
					}
				}
				transfromed_curTupleList.add(tranformedStr + "|" + ((String) x.get(attrNumber - 1)));
				temp.add(tranformedStr);

				return temp;

			}
		}).cache();
		// System.out.println(line.collect());
		System.out.println("map process success");

		JavaRDD<String> line2 = line.map(new Function<ArrayList, String>() {

			public String call(ArrayList x) throws Exception {
				String ill = "";
				String line = "|";
				StringTokenizer kts = new StringTokenizer(x.get(0).toString(), ",");
				StringTokenizer kts2 = new StringTokenizer(kts.nextToken(), "|");
				while (true) {
					if (kts2.hasMoreTokens()) {
						ill = kts2.nextToken();
						line = line + ill + "|";
						continue;
					}

					else
						break;
				}

				return line;

			}

		});

		// System.out.println(line2.collect());
		System.out.println("map2 process success");

		JavaPairRDD<String, Integer> line3 = line2.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2(x, 1);
			}
		});

		// System.out.println(line3.collect());
		System.out.println("mapToPair process success");

		line4 = line3.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) throws Exception {
				kts2.add(x+y);
				return x + y;
			}
		});

		System.out.println(line4.collect());
		System.out.println("reduceByKey process success");
		System.out.println(kts2.toString());
		// .partitionBy(new HashPartitioner(100)
		System.out.println(stopFlag);
		kts2.clear();
		
		int count = (int) line4.count() - 1;
		while (count >= 0) {
			int i = line4.collect().get(count)._2();

			if (i < this.KValue) {
				stopFlag = false;
				return stopFlag;
			}
			count--;
		}

		// line4.saveAsTextFile("output_folder_" + count++);
		return stopFlag;
	}

	public void performAnonymity() {
		System.out.println("performAnonymity start!!");
		JavaRDD<ArrayList> info = sc1.parallelize(tupleList_T1).persist(StorageLevel.MEMORY_ONLY());

		ArrayList<ArrayList<Integer>> nodeQueue = new ArrayList<ArrayList<Integer>>();
		HashMap<String, Integer> duplicateList = new HashMap<String, Integer>();

		ArrayList<Integer> initNode = new ArrayList<Integer>();

		for (int i = 0; i < this.projectionList.size() - 1; ++i)
			initNode.add(new Integer(0));

		// initialize
		nodeQueue.add(initNode);

		int curCount = 0;
		while (nodeQueue.size() > 0) {
			ArrayList<Integer> curNode = nodeQueue.remove(0);

			// Perform anonymization
			if (curCount > 0) {
				this.transfromed_tupleList_T1.clear();

				if ((performGeneralization(curNode, this.tupleList_T1, this.transfromed_tupleList_T1, info))) {
					// System.out.println("curNode : " + curNode);
					System.out.println("per success");

					if (this.exIR == 0)
						this.exIR = this.curIR;
					if (this.exIR * 2 < this.curIR) {
						// System.out.println("***curIR is too high" + curNode + " : " + this.curIR);
						return;
					}

					// System.out.println(curNode + "\t" + this.curIR);
					if (this.curIR <= this.exIR) {
						// System.out.println(this.curIR + " <- " + this.exIR);
						this.exIR = this.curIR;
						this.curIR = 0.0;
						this.IRcnt = 0;

						;
						// Writer outWriter_T1 = textUtil.createTXTFile(
						// Parameter2.k_anonymous_tree_T1 + KValue + "_" + Parameter2.dataSize +
						// "1.txt");
						//
						// for (int i = 0; i < this.transfromed_tupleList_T1.size(); ++i)
						// textUtil.writeString(outWriter_T1, this.transfromed_tupleList_T1.get(i) +
						// "\n");
						//
						// textUtil.saveTXTFile(outWriter_T1);

						this.fitNode = (ArrayList<Integer>) (curNode.clone());

					}
					return;
				} // if end

			} // if end

			// add next nodes
			for (int i = 0; i < this.projectionList.size() - 1; ++i) {
				ArrayList<Integer> tempNode = (ArrayList<Integer>) (curNode.clone());

				Integer attrMaxValue = this.maxMap.get(this.projectionList.get(i));
				if (attrMaxValue >= (tempNode.get(i).intValue() + 1)) {

					tempNode.set(i, new Integer(tempNode.get(i).intValue() + 1));

					String tempStr = new String();

					for (int j = 0; j < this.projectionList.size() - 1; ++j)
						tempStr = tempStr + "_" + tempNode.get(j);
					Object tempObj = duplicateList.get(tempStr);
					if (tempObj == null) {
						nodeQueue.add(tempNode);
						duplicateList.put(tempStr, new Integer(0));
					}
				}
			}
			++curCount;
		}
		System.out.println("performAnonymity Finish!!");
	}

	public String run() {

		System.out.println("============================================================");
		System.out.println("k    : " + Parameter2.KValue);
		System.out.println("============================================================");

		loadGenTree();
		loadData(this.inputFile_T1, this.tupleList_T1);

		performAnonymity();

		return KValue + "\t" + this.fitNode + "\t" + this.exIR;
		// return null;
	}

	static HashMap<Integer, String> need_gTree = new HashMap<Integer, String>();
	static ArrayList<Integer> need_att = new ArrayList<Integer>();

	// main part
	public static void main(String[] args) {
		// System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-2.8.4");
		need_gTree.put(1, "age");
		need_gTree.put(2, "sex");
		need_gTree.put(3, "surgery");
		need_gTree.put(4, "length");
		need_gTree.put(5, "location");
		Scanner scan = new Scanner(System.in);
		long start = System.currentTimeMillis();
		// System.out.println("출력 속성 개수 입력");
		// int num=scan.nextInt();
		int num = 2;
		// for(int i=0;i<num;i++) {
		// //need_att.add(scan.nextInt());
		//
		// }
		need_att.add(1);
		need_att.add(5);

		if (num == 0) {
			for (int i = 0; i < 5; i++) {
				need_att.add(i + 1);
			}
		}

		kAnonymity_spark_local mykAnonymity = new kAnonymity_spark_local();

		System.out.println("projectionList : " + projectionList);

		mykAnonymity.run();
		System.out.println("***** Done ***** ");
		System.out.println(line4.collect());
		long end = System.currentTimeMillis();

		System.out.println("running time : " + (end - start) / 1000.0);
	}

}