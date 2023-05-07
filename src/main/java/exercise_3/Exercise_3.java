package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {
    static final Map<Long, String> labels = ImmutableMap.<Long, String>builder()
            .put(1l, "A")
            .put(2l, "B")
            .put(3l, "C")
            .put(4l, "D")
            .put(5l, "E")
            .put(6l, "F")
            .build();

    private static class VProg extends AbstractFunction3<Long,PathTup,PathTup,PathTup> implements Serializable {
        @Override
        public PathTup apply(Long vertexID, PathTup vertexValue, PathTup message) {
            if (message.equals(new PathTup(Integer.MAX_VALUE))) {             // superstep 0
                return vertexValue;
            } else {                                        // superstep > 0
//                System.out.println("vertexId"+vertexID+" vertexValue "+vertexValue+" message "+message);
                if (vertexValue._1 >= message._1) {
                    return message;
                }
                else {
                    return vertexValue;
                }
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<PathTup,Integer>, Iterator<Tuple2<Object,PathTup>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, PathTup>> apply(EdgeTriplet<PathTup, Integer> triplet) {
            Long sourceVertex = triplet.srcId();
            Integer edge_value = triplet.attr();

            String sourceNode = labels.get(sourceVertex);
            PathTup inputVertex = triplet.srcAttr();

            // the node not accessed yet by A, so no message could be sent
            if (inputVertex.equals(new PathTup(Integer.MAX_VALUE)) || triplet.dstAttr() <= inputVertex) {
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,PathTup>>().iterator()).asScala();
            }
            else {
                // the node has weight, so was reached by A, send that to the neighboring nodes
                List<String> path = new ArrayList<String>(inputVertex._2);
                path.add(sourceNode);
                PathTup to_send = new PathTup(inputVertex._1 + edge_value, path);
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object, PathTup>(triplet.dstId(), to_send)).iterator()).asScala();
            }
        }
    }


    private static class merge extends AbstractFunction2<PathTup,PathTup,PathTup> implements Serializable {
        @Override
        public PathTup apply(PathTup o, PathTup o2) {
            if (o._1 >= o2._1) {
                return o2;
            }
            else {
                return o;
            }
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {

        List<Tuple2<Object,PathTup>> vertices = Lists.newArrayList(
                new Tuple2<Object,PathTup>(1l,new PathTup(0)),
                new Tuple2<Object,PathTup>(2l,new PathTup(Integer.MAX_VALUE)),
                new Tuple2<Object,PathTup>(3l,new PathTup(Integer.MAX_VALUE)),
                new Tuple2<Object,PathTup>(4l,new PathTup(Integer.MAX_VALUE)),
                new Tuple2<Object,PathTup>(5l,new PathTup(Integer.MAX_VALUE)),
                new Tuple2<Object,PathTup>(6l,new PathTup(Integer.MAX_VALUE))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object,PathTup>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<PathTup,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new PathTup(), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(PathTup.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(PathTup.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(new PathTup(Integer.MAX_VALUE),
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        ClassTag$.MODULE$.apply(PathTup.class))
                .vertices()
                .toJavaRDD()
                .foreach(v -> {
                    Tuple2<Object,PathTup> vertex = (Tuple2<Object,PathTup>)v;

                    // node was reached but not recorded to its path
                    // have to add it
                    String srcNode = labels.get(vertex._1);
                    List<String> path = vertex._2._2;
                    path.add(srcNode);
                    System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+path+" with cost "+vertex._2._1);
                });
    }

}