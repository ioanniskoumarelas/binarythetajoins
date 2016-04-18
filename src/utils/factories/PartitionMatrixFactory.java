/**
 * PartitionMatrixFactory.java
 * 
 * It is responsible for creating a PartitionMatrix object according to the
 * histograms and the parameters provided. It creates random queries which
 * in practice could represent real user queries for the system's needs.
 * 
 * @author John Koumarelas
 */
package utils.factories;

import java.util.Arrays;
import java.util.Random;

import model.BucketBoundaries;
import model.PartitionMatrix;
import datatypes.IntPair;
import datatypes.LongPair;

public class PartitionMatrixFactory {
	
	private int buckets; // Affects the size of the PM
	
	private int bands;
	private int sparsity;
	private int bandsOffsetSeed;
	
	private long executionTime;
	
	/*	Relation S	*/
	private BucketBoundaries[] boundariesS;
	private long[] countsS;
	private long sizeS;
	
	/*	Relation T	*/
	private BucketBoundaries[] boundariesT;
	private long[] countsT;
	private long sizeT;
	
	public PartitionMatrixFactory(int buckets, BucketBoundaries[] boundariesS, BucketBoundaries[] boundariesT, 
			long[] countsS, long[] countsT, long sizeS, long sizeT,int bands, int sparsity, int bandsOffsetSeed) {
		
		this.buckets = buckets;
		this.sizeS = sizeS;
		this.sizeT = sizeT;
		this.bands = bands;
		this.sparsity = sparsity;
		this.bandsOffsetSeed = bandsOffsetSeed;
		
		this.boundariesS = boundariesS;
		this.boundariesT = boundariesT;
		this.countsS = countsS;
		this.countsT = countsT;
	}
	
	/**
	 * "_" -> splits the bands of the query
	 * "|" -> sets the threshold of every band
	 * 
	 * example [10|15]_[20|25] --> two long pairs: (10,15),(20,25)
	 */
	private static LongPair[] getQueryParts(String query) {
		String[] queryToks = query.trim().split("_");
		
		LongPair[] queryParts = new LongPair[queryToks.length];
		
		for (int i = 0 ; i < queryToks.length; ++i) {
			String[] toks = queryToks[i].substring(1,queryToks[i].length()-1).split("\\|");
			queryParts[i] = new LongPair();
			queryParts[i].set(Long.valueOf(toks[0]), Long.valueOf(toks[1]));
		}
		
		return queryParts;
	}
	
	/**
	 * Creates a random query using the parameters provided. The method tries to separate the bands,
	 * by assigning only a specific range in which it can be produced. 
	 * @param bands: number of bands
	 * @param sparsity: the width of each band
	 * @param bandsOffsetSeed:	helps to produce different matrices
	 * @param buckets: size of the PartitionMatrix
	 * @param boundariesS: boundaries of relation S
	 * @param boundariesT: boundaries of relation T
	 * @return
	 */
	public static String getQuery(int bands, int sparsity, int bandsOffsetSeed, int buckets,
			BucketBoundaries[] boundariesS,BucketBoundaries[] boundariesT) {
		StringBuffer sb = new StringBuffer();
		
		Random r = new Random(bandsOffsetSeed);
		
		/*	The first value is always the same, so we've got to get rid of it	*/
		r.nextDouble();
		
		double totalBandRange = 2 * buckets;
		double perBandRange = totalBandRange / bands;
		
		int allowedC1BandRange = (int)perBandRange - sparsity;
		
		for (int i = 0; i < bands; ++i) {
			double random = r.nextDouble();
			int c1 = (int) ((i * perBandRange) + (random * 0.999999999) * allowedC1BandRange);
			int c2 = c1 + sparsity;
			
			sb.append("[");
			
			if (c1 < buckets && c2 < buckets) {
				sb.append(-1 * boundariesS[(buckets-1) - c1].getFrom());
				sb.append("|");
				sb.append(-1 * boundariesS[(buckets-1) - c2].getTo());
			} else if (c1 < buckets && c2 >= buckets) {
				// Switched order of printing values as negative should be at the lower bound.
				sb.append(-1 * boundariesS[(buckets-1) - c1].getFrom());
				sb.append("|");
				sb.append(boundariesT[c2 - buckets].getTo());
			} else if (c1 >= buckets && c2 >= buckets) {
				// Switched order of printing values as lower negative value should be at the lower bound.
				sb.append(boundariesT[c1 - buckets].getFrom());
				sb.append("|");
				sb.append(boundariesT[c2 - buckets].getTo());
			}

			sb.append("]");
						
			if ((i + 1) < bands){
				sb.append("_");
			}
		}

		System.out.println(sb.toString());
		return sb.toString();
	}
	
	/**
	 * This is a more sophisticated way to determine the candidate cells from the non-candidate cells
	 * of the PartitionMatrix. It does that by not looping through all the files of the two relationships,
	 * but instead it uses merely the first and last value of each bucket to answer that question.
	 * 
	 * Consider the following cases:
	 * 
	 *	i: a bucket index from relation S
	 *	j: a bucket index from relation T
	 *
	 *	e.g.: X[k].first: the first value of relation X of the bucket with the index k
	 *
	 *	Case 1.		S[i].first|--------------|S[i].last
	 *	T[j].first+c1|--------------------------|T[j].last+c2
	 *	
	 *	T[j].first + c1: is the left-most value of the range of the bucket from T.
	 *	T[j].last + c2: is the right-most value of the range of the bucket from T.
	 *
	 *	Explanation: The bucket from relation S is between the bucket from relation T,
	 *	for the current query (c1,c2).
	 * 	
	 * 	Example:	
	 * S[i]:	[20,25]
	 * T[j]:	[5,15]
	 * c1:	1
	 * c2:	15
	 * 
	 * T[j].first+c1	<=	S[i].first	AND	S[i].last	<=	T[j].last+c2
	 * 5+1	<=	20 AND	25	<=	30
	 * TRUE
	 * 
	 * Accordingly the other cases:
	 * 
	 * Case 2.	S[i].first|---------------------|S[i].last
	 * 			T[j].first+c1|------|T[j].last+c2
	 * 
	 * Explanation:	Bucket from T is between bucket of relation S (for the current query).
	 * 
	 * Case 3.		S[i].first|---------------------|S[i].last
	 * 	T[j].first+c1|----------------|T[j].last+c2
	 * 
	 * Explanation: There is an overlapping between the two buckets. (for the current query)
	 * 
	 * Case 4.		S[i].first|---------------------|S[i].last
	 * 					T[j].first+c1|----------------|T[j].last+c2
	 * 
	 * 
	 * @return: True: Candidate Cell, False: otherwise
	 */
	private boolean evaluateQueryHistogramsEnds(LongPair queryPart, BucketBoundaries boundaryS, BucketBoundaries boundaryT) {
		long c1 = queryPart.getFirst();
		long c2 = queryPart.getSecond();
		
		long bndTFirstC1 = boundaryT.getFrom() + c1;
		long bndTLastC2 = boundaryT.getTo() + c2;
		
		long bndSFirst = boundaryS.getFrom();
		long bndSLast = boundaryS.getTo();
		
		/* 
			If: The first value from T is lower than the last value from S AND
				the last value from T is bigger than the first value of S
			Then: Candidate Cell --> True
		*/
		return bndTFirstC1 <= bndSLast && bndSFirst <= bndTLastC2;
	}
	
	/**
	 * @deprecated: Slow
	 * 
	 * This method tries each possible combination from the two buckets to answer the question.
	 * Good in cases where we want to be sure if we produce correct results in case of a custom UDF 
	 * for the join. Therefore we leave it here for testing purposes.
	 * 
	 * @return: True: Candidate Cell, False: otherwise
	 */
	private boolean evaluateQueryBruteForce(LongPair queryPart, BucketBoundaries boundaryS, BucketBoundaries boundaryT) {
		long c1 = queryPart.getFirst();
		long c2 = queryPart.getSecond();
		
		for (long valueS = boundaryS.getFrom(); valueS <= boundaryS.getTo(); ++valueS ) {
			for (long valueT = boundaryT.getFrom(); valueT <= boundaryT.getTo(); ++valueT){
				
				// T.a + c1 <= S.a <= T.a + c2
				if ((valueT + c1 <= valueS) && (valueS <= valueT + c2)){
					return true;
				}
				
			}
		}
		return false;
	}
	
	private PartitionMatrix createPartitionMatrix() {
		long[][] M = new long[boundariesS.length][boundariesT.length];
		
		/*	Matrix initialization	*/
		for (int i = 0 ; i < M.length ; ++i) {
			Arrays.fill(M[i], 0L);
		}
		
		LongPair[] queryParts = getQueryParts(getQuery(bands,sparsity,bandsOffsetSeed,buckets,boundariesS,boundariesT));
		
		/*
		 * Fill the cells with values.
		 * - Sum: For input cost
		 * - Multiplication: For output cost
		 */
		for (int i = 0 ; i<boundariesS.length ; ++i) {
			for (int j = 0 ; j < boundariesT.length ; ++j) {
				for (int q = 0 ; q < queryParts.length ; ++q) {
					
					/*	Use the following evaluation method especially in cases of a custom join UDF	*/
//					if(evaluateQueryBruteForce(queryParts[q],boundariesS[i],boundariesT[j])){
					
					if(evaluateQueryHistogramsEnds(queryParts[q],boundariesS[i],boundariesT[j])){
						long sum = 0;
						
						IntPair cell = new IntPair();
						cell.set(i, j);
						
						sum = countsS[i] + countsT[j]; // Sum: input cost

						M[i][j] = sum;
						
						break;
					}
				}
			}
		}
		
		PartitionMatrix pm = new PartitionMatrix();
		pm.setMatrix(M);
		pm.setBoundariesS(boundariesS);
		pm.setBoundariesT(boundariesT);
		pm.setCountsS(countsS);
		pm.setCountsT(countsT);
		pm.setSizeS(sizeS);
		pm.setSizeT(sizeT);
				
		return pm;
	}
	
	public PartitionMatrix getPartitionMatrix() {
		long start = System.currentTimeMillis();
		PartitionMatrix pm = createPartitionMatrix();
		long end = System.currentTimeMillis();
		executionTime = end-start;
		
		return pm;
	}
	
	public long getExecutionTime() {
		return executionTime;
	}
}
