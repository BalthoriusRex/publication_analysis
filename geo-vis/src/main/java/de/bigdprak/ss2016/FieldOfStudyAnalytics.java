package de.bigdprak.ss2016;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import de.bigdprak.ss2016.database.FieldOfStudy;
import de.bigdprak.ss2016.database.FieldOfStudyHierarchy;
import de.bigdprak.ss2016.database.Location;
import de.bigdprak.ss2016.database.PaperAuthorAffiliation;
import de.bigdprak.ss2016.database.PaperKeyword;
import de.bigdprak.ss2016.utils.SparkUtility;

/**
 * Klasse zur Beantwortung von Anfragen wie
 * "Welche Affiliations haben im Bereich Mathematik gemeinsam publiziert?"
 */
public class FieldOfStudyAnalytics {
	
	public static void main(String[] args) {
		String topic = null;
		
		String topicGiven;
		//topicGiven = "Rasterisation";
		//topicGiven = "Mitoplast";
		topicGiven = "Mathematics";
		//topicGiven = "Risk analysis";
		
		String topicFromCommandline = extractTopicFromCommandline(args);
		if (topicFromCommandline == null) {
			topic = topicGiven;
		} else {
			topic = topicFromCommandline;
		}
		
		// init Spark
		SparkUtility.init(args, true, true);
		
		// berechne Ko-Authorschaften
		getCoAuthorShips(topic);
		
		// close Spark
		SparkUtility.close();
	}
	
	/**
	 * Extrahiert Topic-Informationen auf dem Commandline-Input.
	 * Topics, die aus bis zu fünf Wörtern bestehen, werden dabei erkannt.
	 * Weitere Inhalte werden abgeschnitten.
	 * @param args
	 * 		Commandoline-Parameter, erwartet an Stelle 1 das Wort "remote" und überspringt dieses daher.
	 * @return
	 * 		Topic aus Commandline Index 2 bis 6 oder null
	 */
	private static String extractTopicFromCommandline(String[] args) {
		String topicFromCommandline = null;
		
		String input = "";
		try { input +=       args[2]; } catch (ArrayIndexOutOfBoundsException e) {}
		try { input += " " + args[3]; } catch (ArrayIndexOutOfBoundsException e) {}
		try { input += " " + args[4]; } catch (ArrayIndexOutOfBoundsException e) {}
		try { input += " " + args[5]; } catch (ArrayIndexOutOfBoundsException e) {}
		try { input += " " + args[6]; } catch (ArrayIndexOutOfBoundsException e) {}
		
		if (input.equals("")) {
			String msg = ""
					+ "\n"
					+ "No research topic specified...\n"
					+ "Using default topic...\n";
			System.out.println(msg);
		} else {
			topicFromCommandline = input;
		}
		
		return topicFromCommandline;
	}
	
	/**
	 * Berechnet Ko-Autorschaften aus Tabellen, die Spark vorher zur Verfügung gestellt wurden.
	 * Die ermittelten Ko-Autorschaften werden in eine Datei geschrieben, deren Name sich
	 * aus dem übergebenen Topic ergibt.
	 * Dateiname: edges_by_field_on_TOPIC.txt
	 * Leerzeichen, die ggf. im Topic enthalten sind, werden durch Underscore ("_") ersetzt.
	 * @param topic
	 * 		Themengebiet, das Untersucht werden soll
	 */
	public static void getCoAuthorShips(String topic) {
		
		// implementiert eine vollständige Spark-Pipeline zur Berechnung der Ko-Autorschaften
		
		String t_FieldOfStudy = FieldOfStudy.class.getSimpleName();
		String t_FieldOfStudyHierarchy = FieldOfStudyHierarchy.class.getSimpleName();
		String t_PaperKeyword = PaperKeyword.class.getSimpleName();
		String t_PaperAuthorAffiliation = PaperAuthorAffiliation.class.getSimpleName();
		String t_Location = Location.class.getSimpleName();
		
		System.out.println("[JOB] computing edges for field of study: " + topic);
		
		String id_FoS    = "fieldOfStudyID";
		String id_FoSH_c = "childFieldOfStudyID";
		String id_FoSH_p = "parentFieldOfStudyID";
		String id_map = "fieldOfStudyIDmappedToKeyword";
		String pID = "paperID";
		String affID = "affiliationID";
		String paa_affName = "normalizedAffiliationName";
		String loc_affName = "name";
		
		SQLContext sql = SparkUtility.getSQL();
		
		long t_start = System.currentTimeMillis();
		
		DataFrame df_FoS  = sql.table(t_FieldOfStudy);
		DataFrame df_FoSH = sql.table(t_FieldOfStudyHierarchy)
		                       .select(id_FoSH_c, id_FoSH_p);
		DataFrame df_PKW  = sql.table(t_PaperKeyword)
		                       .select(pID, id_map);
		DataFrame df_PAA  = sql.table(t_PaperAuthorAffiliation)
		                       .select(pID, affID, paa_affName);
		DataFrame df_Loc  = sql.table(t_Location);
		
		// wähle FoS-Einträge, die dem Topic entsprechen
		DataFrame df_child_0 = df_FoS
				.filter("fieldOfStudyName LIKE '" + topic + "'")
				.select(id_FoS);
		//df_child_0.show();
		
		// JOIN mit FoSH, um Kind-Elemente ersten Grades zu finden
		DataFrame df_child_1 = df_child_0
				.join(df_FoSH, df_child_0.col(id_FoS).equalTo(df_FoSH.col(id_FoSH_p)))
				.select(df_FoSH.col(id_FoSH_c))
				.toDF(id_FoS);
		//df_child_1.show();
		
		// JOIN mit FoSH, um Kind-Elemente zweiten Grades zu finden
		DataFrame df_child_2 = df_child_1
				.join(df_FoSH, df_child_1.col(id_FoS).equalTo(df_FoSH.col(id_FoSH_p)))
				.select(df_FoSH.col(id_FoSH_c))
				.toDF(id_FoS);
		//df_child_2.show();
		
		// JOIN mit FoSH, um Kind-Elemente dritten Grades zu finden
		DataFrame df_child_3 = df_child_2
				.join(df_FoSH, df_child_2.col(id_FoS).equalTo(df_FoSH.col(id_FoSH_p)))
				.select(df_FoSH.col(id_FoSH_c))
				.toDF(id_FoS);
		//df_child_3.show();
		
		// UNION aller Kind-Resultate, sodass eine Liste mit FoS-IDs für das gesuchte Topic
		// und alle davon abgeleiteten Kind-Elemente entsteht
		DataFrame df_all_FoS_IDs = df_child_0
				.unionAll(df_child_1)
				.unionAll(df_child_2)
				.unionAll(df_child_3)
				.distinct();
		//df_all_FoS_IDs.show();
		
		// übersetze die FoS-IDs in PaperIDs durch JOIN mit PaperKeywords
		DataFrame df_pIDs = df_all_FoS_IDs
				.join(df_PKW, df_all_FoS_IDs.col(id_FoS).equalTo(df_PKW.col(id_map)))
				.select(df_PKW.col(pID))
				.toDF(pID);
		//df_pIDs.show();
		
		// bereite PAA-Tabelle (PaperAuthorAffiliations) vor
		DataFrame df_PAA_filtered = df_PAA;
		// behalte nur solche PAA-Zeilen, von denen Informationen zu den Orten vorliegen
		df_PAA_filtered = df_PAA_filtered
				.join(df_Loc, df_PAA_filtered.col(paa_affName).equalTo(df_Loc.col(loc_affName)))
				.select(pID, affID, paa_affName);
		// behalte nur solche PAA-Zeilen, deren pID zum FieldOfStudy passt
		df_PAA_filtered = df_PAA_filtered
				.join(df_pIDs, df_PAA_filtered.col(pID).equalTo(df_pIDs.col(pID)))
				.drop(df_pIDs.col(pID));
		//df_PAA_filtered.show();
		
		
		// berechne Ko-Autorschaften aus PAA-Tabelle
		df_PAA_filtered.registerTempTable("PAA");
		DataFrame df_edges_agg = sql.sql(""
					+ "SELECT "
						+ "A."+affID+" AS affID_A,  "
						+ "B."+affID+" AS affID_B,  "
						+ "COUNT(A."+affID+", B."+affID+") AS anzahl "
					+ "FROM "
						+ "PAA A JOIN "
						+ "PAA B "
						+ "ON A.paperID = B.paperID "
					+ "WHERE NOT(A."+affID+" = B."+affID+") "
					+ "GROUP BY "
						+ "A."+affID+", B."+affID+""
					+ "");
		df_edges_agg.show();
		
		String target_repl = topic.replace(" ", "_");
		String file = "edges_by_field_on_" + target_repl + ".txt";
		String path = SparkUtility.getFolderHadoop() + file;
		
		// print results ...
		SparkUtility.printResults(path, df_edges_agg.collect());
		//df_edges_agg.toJavaRDD().repartition(1).saveAsTextFile(path);
		
		
		
		long t_end = System.currentTimeMillis();

		long ms = t_end - t_start;
		long s = ms / 1000;
		long m = s / 60;
		long h = m / 60;
		ms = ms - s * 1000;
		s = s - m * 60;
		m = m - h * 60;
		System.out.println("[DURATION] " + h + "h " + m + "m " + s + "s " + ms + "ms");
		System.out.println("printed results to file " + file);
		
	}
}
