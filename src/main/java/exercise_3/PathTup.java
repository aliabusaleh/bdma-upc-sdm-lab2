package exercise_3;

import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;

public class PathTup extends Tuple2<Integer, List<String>> {
    public PathTup() {super(0, new ArrayList<String>()); }
    public PathTup(Integer num) { super(num, new ArrayList<String>()); }
    public PathTup(Integer num, List<String> str_list) { super(num, str_list); }

    boolean equals(PathTup pathtup) { return this._1.equals(pathtup._1) && this._2.equals(pathtup._2); }
}
