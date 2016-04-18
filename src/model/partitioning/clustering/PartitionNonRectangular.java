/**
 * PartitionNonRectangular.java
 * 
 * This Partition allows forming non-rectangular regions,
 * which do not have any limitations as to how they will
 * be formed.
 * 
 * @author John Koumarelas
 */

package model.partitioning.clustering;

import model.PartitionMatrix;
import model.partitioning.Partition;

public class PartitionNonRectangular extends Partition {
	
	public PartitionNonRectangular() {
		super();
	}
	
	public PartitionNonRectangular(int id, PartitionMatrix pm) {
		super(id,pm);
	}

	public void addPartition(PartitionNonRectangular other) {
		this.candidateS.addAll(other.candidateS);
		this.candidateT.addAll(other.candidateT);
		this.candidateCells.addAll(other.candidateCells);
		this.changed = true;
	}
	
	@Override
	public String toString() {
		return "id: " + id + " inputCost: " + computeInputCost() + "\n" + super.toString();
	}
}
