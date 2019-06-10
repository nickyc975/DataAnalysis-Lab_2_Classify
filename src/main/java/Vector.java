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

    public Vector(int dim, double value) {
        this.vector = new double[dim];
        Arrays.fill(this.vector, value);
    }

    public int dim() {
        return this.vector.length;
    }

    public double get(int index) {
        return this.vector[index];
    }

    public void set(int index, double value) {
        this.vector[index] = value;
    }

    public double mod() {
        double result = 0;
        for (double value : this.vector) {
            result += value * value;
        }
        return Math.sqrt(result);
    }

    public Vector add(Vector that) {
        check(this, that);
        for (int i = 0; i < this.dim(); i++) {
            this.vector[i] += that.vector[i];
        }
        return this;
    }

    public Vector sub(Vector that) {
        check(this, that);
        for (int i = 0; i < this.dim(); i++) {
            this.vector[i] -= that.vector[i];
        }
        return this;
    }

    public Vector mul(double alpha) {
        for (int i = 0; i < this.dim(); i++) {
            this.vector[i] *= alpha;
        }
        return this;
    }

    public Vector div(double alpha) {
        for (int i = 0; i < this.dim(); i++) {
            this.vector[i] /= alpha;
        }
        return this;
    }

    public double dot(Vector that) {
        check(this, that);
        double result = 0;
        for (int i = 0; i < this.dim(); i++) {
            result += this.vector[i] * that.vector[i];
        }
        return result;
    }

    public static double mod(Vector vector) {
        return vector.mod();
    }

    public static Vector add(Vector x, Vector y) {
        return x.clone().add(y);
    }

    public static Vector sub(Vector x, Vector y) {
        return x.clone().sub(y);
    }

    public static Vector mul(Vector vector, double alpha) {
        return vector.clone().mul(alpha);
    }

    public static Vector div(Vector vector, double alpha) {
        return vector.clone().div(alpha);
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
        if (x.dim() != y.dim()) {
            throw new IllegalArgumentException("Different dims: " + x.dim() + " and " + y.dim());
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
            if (that.dim() != this.dim()) {
                return false;
            } else {
                for (int i = 0; i < this.dim(); i++) {
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