package datatypes;

public class DoubleQuad implements Comparable<DoubleQuad>{
	
	private double first;
	private double second;
	private double third;
	private double fourth;
	
	public DoubleQuad() {
		this.set(Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE);
	}

	public void set(double first, double second, double third, double fourth) {
		this.first = first;
		this.second = second;
		this.third = third;
		this.fourth = fourth;
	}
	
	public DoubleQuad(double first, double second, double third, double fourth) {
		this.set(first, second, third, fourth);
	}

	@Override
	public boolean equals(Object right) {
		if (right instanceof DoubleQuad) {
			DoubleQuad r = (DoubleQuad) right;
			return r.first == first && r.second == second && r.third == third && r.fourth == fourth;
		} else {
			return false;
		}
	}

	@Override
	public int compareTo(DoubleQuad o) {
		if (first != o.first) {
			return first < o.first ? -1 : 1;
		} else if (second != o.second) {
			return second < o.second ? -1 : 1;
		} else if (third != o.third) {
			return third < o.third ? -1 : 1;
		} else if (fourth != o.fourth ) {
			return fourth < o.fourth ? -1 : 1;
		} else {
			return 0;
		}
	}
	
	@Override
	public String toString() {
		return "[" + first + ","  + second + "," + third + "," + fourth + "]";
	}
	
	/*	Getters - Setters	*/
	
	public void setFirst(double first) {
		this.first = first;
	}

	public void setSecond(double second) {
		this.second = second;
	}

	public void setThird(double third) {
		this.third = third;
	}

	public void setFourth(double fourth) {
		this.fourth = fourth;
	}
	
	public double getFirst() {
		return first;
	}

	public double getSecond() {
		return second;
	}

	public double getThird() {
		return third;
	}
	
	public double getFourth() {
		return fourth;
	}
}
