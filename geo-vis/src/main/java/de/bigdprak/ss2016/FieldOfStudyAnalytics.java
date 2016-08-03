package de.bigdprak.ss2016;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import de.bigdprak.ss2016.database.FieldOfStudy;
import de.bigdprak.ss2016.database.FieldOfStudyHierarchy;
import de.bigdprak.ss2016.database.Location;
import de.bigdprak.ss2016.database.PaperAuthorAffiliation;
import de.bigdprak.ss2016.database.PaperKeyword;

public class FieldOfStudyAnalytics {
	
	public static void main(String[] args) {
		
		String t_FieldOfStudy = FieldOfStudy.class.getSimpleName();
		String t_FieldOfStudyHierarchy = FieldOfStudyHierarchy.class.getSimpleName();
		String t_PaperKeyword = PaperKeyword.class.getSimpleName();
		String t_PaperAuthorAffiliation = PaperAuthorAffiliation.class.getSimpleName();
		String t_Location = Location.class.getSimpleName();
		String target;
		//target = "Rasterisation";
		//target = "Mitoplast";
		target = "Mathematics";
		
		System.out.println("[JOB] computingn edges for field of study: " + target);
		
		String id_FoS    = "fieldOfStudyID";
		String id_FoSH_c = "childFieldOfStudyID";
		String id_FoSH_p = "parentFieldOfStudyID";
		String id_map = "fieldOfStudyIDmappedToKeyword";
		String pID = "paperID";
		String affID = "affiliationID";
		String paa_affName = "normalizedAffiliationName";
		String loc_affName = "name";
		
		SparkUtility.init(args);
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
		
		DataFrame df_child_0 = df_FoS
				.filter("fieldOfStudyName LIKE '" + target + "'")
				.select(id_FoS);
		//df_child_0.show();
		
		DataFrame df_child_1 = df_child_0
				.join(df_FoSH, df_child_0.col(id_FoS).equalTo(df_FoSH.col(id_FoSH_p)))
				.select(df_FoSH.col(id_FoSH_c))
				.toDF(id_FoS);
		//df_child_1.show();
		
		DataFrame df_child_2 = df_child_1
				.join(df_FoSH, df_child_1.col(id_FoS).equalTo(df_FoSH.col(id_FoSH_p)))
				.select(df_FoSH.col(id_FoSH_c))
				.toDF(id_FoS);
		//df_child_2.show();
		
		DataFrame df_child_3 = df_child_2
				.join(df_FoSH, df_child_2.col(id_FoS).equalTo(df_FoSH.col(id_FoSH_p)))
				.select(df_FoSH.col(id_FoSH_c))
				.toDF(id_FoS);
		//df_child_3.show();
		
		DataFrame df_all_FoS_IDs = df_child_0
				.unionAll(df_child_1)
				.unionAll(df_child_2)
				.unionAll(df_child_3)
				.distinct();
		//df_all_FoS_IDs.show();
		
		//df_all_FoS_IDs.join(df_FoS, df_all_FoS_IDs.col(id_FoS).equalTo(df_FoS.col(id_FoS))).drop(df_all_FoS_IDs.col(id_FoS)).show();
		
		DataFrame df_pIDs = df_all_FoS_IDs
				.join(df_PKW, df_all_FoS_IDs.col(id_FoS).equalTo(df_PKW.col(id_map)))
				.select(df_PKW.col(pID))
				.toDF(pID);
		//df_pIDs.show();
		
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
		
		DataFrame df_1 = df_PAA_filtered.as("df1");
		DataFrame df_2 = df_PAA_filtered.as("df2");
		DataFrame df_edges = df_1
				.join(df_2, df_1.col(pID).equalTo(df_2.col(pID)))
				//.where("NOT(df1.affiliationID = df2.affiliationID)")
				.select(df_1.col(paa_affName), df_2.col(paa_affName))
				.toDF("location1", "location2");
		df_edges.show();
		
		DataFrame df_edges_agg = df_edges
				.as("edges")
				.sqlContext()
				.sql(""
						+ "SELECT location1, location2, COUNT(location1, location2) "
						+ "FROM edges "
						+ "GROUP BY location1, location2 "
						+ "ORDER BY Anzahl DESC")
				//.groupBy("location1", "location2")
				//.count()
				.toDF("location1", "location2", "anzahl");
		df_edges_agg.show();
		
		String target_repl = target.replace(" ", "_");
		String path = SparkUtility.getFolderHadoop() + "edges_by_field_on_" + target_repl + ".txt";
		
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
		
		SparkUtility.close();
	}
}
