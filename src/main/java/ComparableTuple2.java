import scala.Tuple2;

public class ComparableTuple2 extends Tuple2<Integer, Double> implements Comparable<Tuple2<Integer, Double>> {
    private static final long serialVersionUID = 90873245908L;

    public ComparableTuple2(Tuple2<Integer, Double> tuple) {
        super(tuple._1, tuple._2);
    }

    @Override
    public int compareTo(Tuple2<Integer, Double> that) {
        if (this == that) {
            return 0;
        }

        if (!this._1.equals(that._1)) {
            return this._1.compareTo(that._1);
        } else {
            return this._2.compareTo(that._2);
        }
    }
}