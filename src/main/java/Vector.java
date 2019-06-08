import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Vector implements Serializable, Cloneable {
    private static final long serialVersionUID = 38947290347269459L;

    private final double[] vector;

    public Vector(double[] vector) {
        this.vector = Arrays.copyOf(vector, vector.length);
    }

    public int size() {
        return this.vector.length;
    }

    public void add(Vector that) {
        check(this, that);
        for (int i = 0; i < this.size(); i++) {
            this.vector[i] += that.vector[i];
        }
    }

    public void sub(Vector that) {
        check(this, that);
        for (int i = 0; i < this.size(); i++) {
            this.vector[i] -= that.vector[i];
        }
    }

    public void mul(double alpha) {
        for (int i = 0; i < this.size(); i++) {
            this.vector[i] *= alpha;
        }
    }

    public void div(double alpha) {
        for (int i = 0; i < this.size(); i++) {
            this.vector[i] /= alpha;
        }
    }

    public double dot(Vector that) {
        check(this, that);
        double result = 0;
        for (int i = 0; i < this.size(); i++) {
            result += this.vector[i] * that.vector[i];
        }
        return result;
    }

    public static Vector zero(int size) {
        double[] vector = new double[size];
        for (int i = 0; i < size; i++) {
            vector[i] = 0;
        }
        return new Vector(vector);
    }

    public static Vector add(Vector x, Vector y) {
        Vector result = x.clone();
        result.add(y);
        return result;
    }

    public static Vector sub(Vector x, Vector y) {
        Vector result = x.clone();
        result.sub(y);
        return result;
    }

    public static Vector mul(Vector vector, double alpha) {
        Vector result = vector.clone();
        result.mul(alpha);
        return result;
    }

    public static Vector div(Vector vector, double alpha) {
        Vector result = vector.clone();
        result.div(alpha);
        return result;
    }

    public static double dot(Vector x, Vector y) {
        return x.dot(y);
    }

    public static Vector fromString(String vectorStr) {
        String[] values = vectorStr.substring(1, vectorStr.length() - 1).split(",");
        double[] vector = Arrays.asList(values).stream().mapToDouble(Double::parseDouble).toArray();
        return new Vector(vector);
    }

    private static void check(Vector x, Vector y) {
        if (x.size() != y.size()) {
            throw new IllegalArgumentException("Different sizes: " + x.size() + " and " + y.size());
        }
    }

    @Override
    public Vector clone() {
        return new Vector(this.vector);
    }

    @Override
    public int hashCode() {
        double sum = 0;
        for (double value : vector) {
            sum += value;
        }
        return (int) sum;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        } else if (! (obj instanceof Vector)) {
            return false;
        } else {
            Vector that = (Vector) obj;
            if (that.size() != this.size()) {
                return false;
            } else {
                for (int i = 0; i < this.size(); i++) {
                    if (that.vector[i] != this.vector[i]) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    @Override
    public String toString() {
        List<String> vectorStr = new ArrayList<>();
        for (double value : vector) {
            vectorStr.add(String.valueOf(value));
        }
        return "[" + String.join(",", vectorStr) + "]";
    }
}