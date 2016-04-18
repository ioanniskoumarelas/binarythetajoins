/**
 * Partition.java
 * 
 * This class implements the concept of a group of cells
 * in a generic way, meaning that they can both be
 * rectangular or any other. It is extended by other
 * groups that follow a specific form (e.g. rectangular).
 * 
 * Each Partition is meant to be directly connect with a
 * different "node" (e.g. in MapReduce: reducer), that will
 * be responsible for checking the combinations of the
 * two relations for the possible results.
 * 
 * @author John Koumarelas
 */

package model.partitioning;

import java.util.HashSet;
import java.util.Iterator;

import datatypes.IntPair;
import model.PartitionMatrix;

public class Partition {

	protected PartitionMatrix pm;
	protected int id; // group/reducer's id
	protected HashSet<Integer> candidateS;
	protected HashSet<Integer> candidateT;
	protected HashSet<IntPair> candidateCells;
	
	protected boolean changed;
	protected long inputCost;
	
	public Partition() {
		this.id = Integer.MIN_VALUE;
		this.candidateS = new HashSet<Integer>();
		this.candidateT = new HashSet<Integer>();
		this.candidateCells = new HashSet<IntPair>();
		this.pm = null;
		this.changed = true;
		this.inputCost = Long.MIN_VALUE;
	}
	
	public Partition(int id, PartitionMatrix pm) {
		this.id = id;
		this.candidateS = new HashSet<Integer>();
		this.candidateT = new HashSet<Integer>();
		this.candidateCells = new HashSet<IntPair>();
		this.pm = pm;
		this.changed = true;
		this.inputCost = Long.MIN_VALUE;
	}
	
	public void addCell(int idxS, int idxT) {
		candidateS.add(idxS);
		candidateT.add(idxT);
		
		candidateCells.add(new IntPair(idxS,idxT));
		
		changed = true;
	}
	
	public long computeInputCost() {
		if (!changed) {
			return inputCost;
		}
		changed = false;
		
		inputCost = 0L;
		
		for (Integer idxS : candidateS) {
			inputCost += pm.getCountsS()[idxS];
		}
		
		for (Integer idxT : candidateT) {
			inputCost += pm.getCountsT()[idxT];
		}

		return inputCost;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		Iterator<Integer> it;
		
		sb.append("S_partitions[");
		it = candidateS.iterator();
		while (it.hasNext()){
			sb.append(it.next());
			if (it.hasNext())
				sb.append(",");
		}
		sb.append("] ");
		
		sb.append("T_partitions[");
		it = candidateT.iterator();
		while (it.hasNext()){
			sb.append(it.next());
			if (it.hasNext())
				sb.append(",");
		}
		sb.append("]");
		
		return sb.toString();
	}
	
	/*
	 * Getters - Setters
	 */
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public HashSet<Integer> getCandidateS() {
		return candidateS;
	}

	public void setCandidateS(HashSet<Integer> candidateS) {
		this.candidateS = candidateS;
	}

	public HashSet<Integer> getCandidateT() {
		return candidateT;
	}

	public void setCandidateT(HashSet<Integer> candidateT) {
		this.candidateT = candidateT;
	}

	public HashSet<IntPair> getCandidateCells() {
		return candidateCells;
	}

	public void setCandidateCells(HashSet<IntPair> candidateCells) {
		this.candidateCells = candidateCells;
	}

	public PartitionMatrix getPm() {
		return pm;
	}

	public void setPm(PartitionMatrix pm) {
		this.pm = pm;
	}
	
}
