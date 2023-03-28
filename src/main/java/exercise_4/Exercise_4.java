package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
		BufferedReader reader;

		try {
			reader = new BufferedReader(new FileReader("src/main/resources/wiki-vertices.txt"));
			String line = reader.readLine();
			java.util.List<Row> vertices_list = new ArrayList<Row>();
			while (line != null) {
				vertices_list.add(RowFactory.create(Long.valueOf(line.split("\t")[0]), line.split("\t")[1]));
//				System.out.println(line);
				// read next line
				line = reader.readLine();
			}
			reader.close();
			// add vertices_rdd
			JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);
			// add vertices schema
			StructType vertices_schema = new StructType(new StructField[]{
					new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
					new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build())
			});
			// create vertices
			Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);

			// edges creation
			java.util.List<Row> edges_list = new ArrayList<Row>();
			reader = new BufferedReader(new FileReader("src/main/resources/wiki-edges.txt"));
			line = reader.readLine();
			while (line != null) {
				edges_list.add(RowFactory.create(Long.valueOf(line.split("\t")[0]), Long.valueOf(line.split("\t")[1])));
				// read next line
				line = reader.readLine();
			}
			reader.close();

			JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);

			StructType edges_schema = new StructType(new StructField[]{
					new StructField("src", DataTypes.LongType, true, new MetadataBuilder().build()),
					new StructField("dst", DataTypes.LongType, true, new MetadataBuilder().build())
			});

			Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

			GraphFrame gf = GraphFrame.apply(vertices,edges);

//			System.out.println(gf);
//
//			gf.edges().show();
//			gf.vertices().show();

			// pageRank calculation
			GraphFrame results = gf.pageRank().resetProbability(0.5).maxIter(3).run();
			Dataset<Row> top10Vertices = results.vertices().orderBy(org.apache.spark.sql.functions.desc("pagerank")).limit(10);
			top10Vertices.show();

			System.out.println("Finish");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
}
