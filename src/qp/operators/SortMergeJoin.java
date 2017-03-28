/** page nested join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class SortMergeJoin extends Join {

	int batchsize; // Number of tuples per out batch

	/**
	 * The following fields are useful during execution of the NestedJoin
	 * operation
	 **/
	int leftindex; // Index of the join attribute in left table
	int rightindex; // Index of the join attribute in right table

	String rfname; // The file name where the right table is materialize

	static int filenum = 0; // To get unique filenum for this operation

	Batch outbatch; // Output buffer
	Batch leftbatch; // Buffer for left input stream, used for subset
	Batch rightbatch; // Buffer for right input stream, used for subset

	ObjectInputStream rightSorted; // File pointer to the right hand
									// materialized sorted file
	ObjectInputStream leftSorted; // File pointer to the right hand materialized
									// sorted file
	String leftSortedFileName;
	String rightSortedFileName;

	int lB; // left page index in leftBlock
	int lE; // left element index in a page
	int rB;
	int rE;

	int lcurs; // Cursor for left side buffer
	int rcurs; // Cursor for right side buffer
	boolean eosl; // Whether end of stream (left table) is reached
	boolean eosr; // End of stream (right table)

	ObjectInputStream inR = null;
	ObjectInputStream inL;

	int numPassLeft = -1; // for getting file name of sorted left
	int numPassRight = -1; // for getting file name of sorted right
	ArrayList<Batch> block;
	ArrayList<Batch> blockLeft;
	ArrayList<Batch> blockRight;

	int halfBlock;

	boolean goToSubsetJoin = false;

	public SortMergeJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}

	/**
	 * During open finds the index of the join attributes Materializes the right
	 * hand side into a file Opens the connections
	 **/

	public boolean open() {

		/** select number of tuples per batch **/
		int tuplesize = schema.getTupleSize();
		batchsize = Batch.getPageSize() / tuplesize;

		Attribute leftattr = con.getLhs();
		Attribute rightattr = (Attribute) con.getRhs();
		leftindex = left.getSchema().indexOf(leftattr);
		rightindex = right.getSchema().indexOf(rightattr);
		Batch rightpage;

		if (!right.open() || left.open()) {
			return false;
		}

		/**
		 * Materialized both left and right into sorted file
		 **/

		try {
			numPassLeft = externalSort(left, "_left", leftindex);
			numPassRight = externalSort(right, "_right", rightindex);

			leftSortedFileName = "merged_left_pass" + numPassLeft
					+ "_subRound0";
			leftSorted = new ObjectInputStream(
					new FileInputStream(leftSortedFileName));

			rightSortedFileName = "merged_right_pass" + numPassRight
					+ "_subRound0";
			rightSorted = new ObjectInputStream(
					new FileInputStream(rightSortedFileName));

		} catch (ClassNotFoundException error) {
			System.out.println("error in opening: " + error);
		} catch (IOException io) {
			System.out.println("error in opening: " + io);
		}

		blockLeft = new ArrayList<Batch>();
		blockRight = new ArrayList<Batch>();
		halfBlock = (numBuff - 1) / 2;

		try {
			for (int i = 0; i < halfBlock; i++) {
				if (leftSorted.available() != 0) {
					blockLeft.add((Batch) leftSorted.readObject());
				} else {
					break;
				}
			}
			for (int i = 0; i < halfBlock; i++) {
				if (rightSorted.available() != 0) {
					blockRight.add((Batch) rightSorted.readObject());
				} else {
					break;
				}
			}

			lB = 0; // left page index in leftBlock
			lE = 0; // left element index in a page
			rB = 0;
			rE = 0;

			lcurs = 0; // Cursor for left side buffer
			rcurs = 0; // Cursor for right side buffer
			eosl = false; // Whether end of stream (left table) is reached
			eosr = true; // End of stream (right table)

			inR = null;

		} catch (ClassNotFoundException error) {
			System.out.println("error in opening phase2: " + error);
		} catch (IOException io) {
			System.out.println("error in opening phase2: " + io);
		}

		return true;

	}

	/**
	 * from input buffers selects the tuples satisfying join condition And
	 * returns a page of output tuples
	 **/

	public Batch next() {
		// System.out.print("NestedJoin:--------------------------in
		// next----------------");
		// Debug.PPrint(con);
		// System.out.println();

		if (!goToSubsetJoin) {
			try {

				outbatch = new Batch(batchsize);

				Tuple l = blockLeft.get(lB).elementAt(lE);
				Tuple r = blockRight.get(rB).elementAt(rE);

				while (!outbatch.isFull()) {
					int result = Tuple.compareTuples(l, r, leftindex,
							rightindex);
					if (result > 0) { ///////// l>r , r= next tuple in right
						if (rE == (blockRight.get(rB).size() - 1)) { //// case1:
																		//// end
																		//// of
																		//// one
																		//// right
																		//// page
							rB++;
							rE = 0;
						} else if (rB == (blockRight.size() - 1)) { //// case2:
																	//// end of
																	//// one
																	//// whole
																	//// right
																	//// block
							blockRight = new ArrayList<Batch>();
							for (int i = 0; i < halfBlock; i++) {// read in
																	// another
																	// block
																	// from
																	// sorted
																	// file
								if (rightSorted.available() != 0) {
									blockRight.add(
											(Batch) rightSorted.readObject());
								} else {
									break;
								}
							}
							rB = 0;
							rE = 0;
						} else { // case3: there are more elements in a page
							rE++;
						}
						r = blockRight.get(rB).elementAt(rE);
					} else if (result < 0) { ///////// l<r , l= next tuple in
												///////// left
						if (lE == (blockLeft.get(lB).size() - 1)) { //// case1:
																	//// end of
																	//// one
																	//// left
																	//// page
							lB++;
							lE = 0;
						} else if (lB == (blockLeft.size() - 1)) { //// case2:
																	//// end of
																	//// one
																	//// whole
																	//// left
																	//// block
							blockLeft = new ArrayList<Batch>();
							for (int i = 0; i < halfBlock; i++) {// read in
																	// another
																	// block
																	// from
																	// sorted
																	// file
								if (leftSorted.available() != 0) {
									blockLeft.add(
											(Batch) leftSorted.readObject());
								} else {
									break;
								}
							}
							lB = 0;
							lE = 0;
						} else { // case3: there are more elements in a page
							lE++;
						}
						l = blockLeft.get(lB).elementAt(lE);
					} else { ///// l==r

						boolean rOver = false; // represent right same tuple
												// exceed current block
						boolean lOver = false; // represent left same tuple
												// exceed current block

						Tuple r2 = blockRight.get(blockRight.size() - 1) // get
																			// last
																			// element
																			// in
																			// last
																			// page
																			// of
																			// rightblock
								.elementAt(blockRight.get(halfBlock - 1).size()
										- 1);
						int compR = Tuple.compareTuples(r, r2, rightindex);

						Tuple l2 = blockLeft.get(blockLeft.size() - 1) // get
																		// last
																		// element
																		// in
																		// last
																		// page
																		// of
																		// leftblock
								.elementAt(blockLeft.get(halfBlock - 1).size()
										- 1);
						int compL = Tuple.compareTuples(l, l2, leftindex);

						if (compR == 0) {
							formSubset("rightSubset", r);
							rOver = true;
						}

						if (compL == 0) {
							formSubset("leftSubset", l);
							lOver = true;
						}

						if (rOver && !lOver) {//// case1: only tuples in right
												//// block overflows
							// write the left pages that contains tuple to a
							// left subset file
							String subset = "leftSubset";
							ObjectOutputStream sub = new ObjectOutputStream(
									new FileOutputStream(subset));
							int offsetEqualB = -1;
							int offsetDiffB = -1;
							int offsetEqualE = -1;
							int offsetDiffE = -1;
							outerloop: for (int i = lB; i < blockLeft
									.size(); i++) {
								for (int j = 0; j < blockLeft.get(i)
										.size(); j++) {
									Tuple t3 = blockLeft.get(i).elementAt(j);
									int compRI = Tuple.compareTuples(l, t3,
											leftindex);
									if (compRI == 0) {
										offsetEqualB = i;
										offsetEqualE = j;
									} else {
										offsetDiffB = i;
										offsetDiffE = j;
										break outerloop;
									}
								}
							}
							// write the pages containing same tuple into a
							// right subset file
							for (int i = 0; i <= offsetEqualB; i++) {
								sub.writeObject((Batch) blockLeft.get(i));
							}

							lB = offsetDiffB;
							lE = offsetDiffE;

							goToSubsetJoin = true;
							return outbatch;

						} else if (!rOver && lOver) {
							//// case2: only tuples in left block overflows
							// write the left pages that contains tuple l a left
							//// subset file
							String subset = "RightSubset";
							ObjectOutputStream sub = new ObjectOutputStream(
									new FileOutputStream(subset));
							int offsetEqualB = -1;
							int offsetDiffB = -1;
							int offsetEqualE = -1;
							int offsetDiffE = -1;
							outerloop: for (int i = rB; i < blockRight
									.size(); i++) {
								for (int j = 0; j < blockRight.get(i)
										.size(); j++) {
									Tuple t3 = blockRight.get(i).elementAt(j);
									int compRI = Tuple.compareTuples(l, t3,
											rightindex);
									if (compRI == 0) {
										offsetEqualB = i;
										offsetEqualE = j;
									} else {
										offsetDiffB = i;
										offsetDiffE = j;
										break outerloop;
									}
								}
							}
							// write the pages containing same tuple into subset
							// file
							for (int i = 0; i <= offsetEqualB; i++) {
								sub.writeObject((Batch) blockRight.get(i));
							}

							rB = offsetDiffB;
							rE = offsetDiffE;

							goToSubsetJoin = true;
							return outbatch;

						} else if (lOver && rOver) {
							////// case3: tuples in both blocks overflow
							goToSubsetJoin = true;
							return outbatch;

						} else {
							//// case4: tuples in both blocks never overflow

							/// r = next tuple in right block to compare with
							/// fixed l
							int offsetEqualBR = -1;
							int offsetDiffBR = -1;
							int offsetEqualER = -1;
							int offsetDiffER = -1;
							outerloop: for (int i = rB; i < blockRight
									.size(); i++) {
								for (int j = 0; j < blockRight.get(i)
										.size(); j++) {
									r2 = blockRight.get(i).elementAt(j);
									if (r2.checkJoin(l, leftindex,
											rightindex)) {
										outbatch.add(r2.joinWith(l));
										offsetEqualBR = i;
										offsetEqualER = j;
									} else {
										offsetDiffBR = i;
										offsetDiffER = j;
										break outerloop;
									}
								}
							}
							/// l = next tuple in left block to compare with
							/// fixed r
							int offsetEqualBL = -1;
							int offsetDiffBL = -1;
							int offsetEqualEL = -1;
							int offsetDiffEL = -1;
							outerloop: for (int i = lB; i < blockLeft
									.size(); i++) {
								for (int j = 0; j < blockLeft.get(i)
										.size(); j++) {
									l2 = blockLeft.get(i).elementAt(j);
									if (l2.checkJoin(r, leftindex,
											rightindex)) {
										outbatch.add(l2.joinWith(r));
										offsetEqualBL = i;
										offsetEqualEL = j;
									} else {
										offsetDiffBL = i;
										offsetDiffEL = j;
										break outerloop;
									}
								}
							}

							lB = offsetDiffBL;
							lE = offsetDiffEL;
							rB = offsetDiffBR;
							rE = offsetDiffER;
						}

					}

				}

			} catch (ClassNotFoundException error) {
				System.out.println(
						"error in reading object from sorted: " + error);
			} catch (IOException io) {
				System.out
						.println("error in reading object from sorted: " + io);
			}

			return outbatch;
		} else {
			return subsetJoin();
		}
	}

	private Batch subsetJoin() {

		int i, j;
		if (eosl) {
			goToSubsetJoin = false;
		}
		outbatch = new Batch(batchsize);
		try {
			inL = new ObjectInputStream(new FileInputStream("leftSubset"));

			while (!outbatch.isFull()) {

				if (lcurs == 0 && eosr == true) {
					/** new left page is to be fetched **/
					leftbatch = (Batch) inL.readObject();
					if (leftbatch == null) {
						eosl = true;
						goToSubsetJoin = false;
						return outbatch;
					}
					/**
					 * Whenver a new left page came , we have to start the
					 * scanning of right table
					 **/
					inR = new ObjectInputStream(
							new FileInputStream("rightSubset"));
					eosr = false;

				}

				while (eosr == false) {

					if (rcurs == 0 && lcurs == 0) {
						rightbatch = (Batch) inR.readObject();
					}
					for (i = lcurs; i < leftbatch.size(); i++) {
						for (j = rcurs; j < rightbatch.size(); j++) {
							Tuple lefttuple = leftbatch.elementAt(i);
							Tuple righttuple = rightbatch.elementAt(j);
							if (lefttuple.checkJoin(righttuple, leftindex,
									rightindex)) {
								Tuple outtuple = lefttuple.joinWith(righttuple);

								// Debug.PPrint(outtuple);
								// System.out.println();
								outbatch.add(outtuple);
								if (outbatch.isFull()) {
									if (i == leftbatch.size() - 1
											&& j == rightbatch.size() - 1) {// case
																			// 1
										lcurs = 0;
										rcurs = 0;
									} else if (i != leftbatch.size() - 1
											&& j == rightbatch.size() - 1) {// case
																			// 2
										lcurs = i + 1;
										rcurs = 0;
									} else if (i == leftbatch.size() - 1
											&& j != rightbatch.size() - 1) {// case
																			// 3
										lcurs = i;
										rcurs = j + 1;
									} else {
										lcurs = i;
										rcurs = j + 1;
									}

								}
							}
						}
						rcurs = 0;
					}

					lcurs = 0;
				}
			}
		} catch (IOException io) {
			System.err.println(
					"NestedJoin:error in reading the subset file: " + io);
		} catch (ClassNotFoundException e) {
			System.err.println(
					"NestedJoin:error in reading the subset file: " + e);
		}
		return outbatch;

	}

	private void formSubset(String setName, Tuple t) {

		try {
			String subset = setName;
			ObjectOutputStream sub = new ObjectOutputStream(
					new FileOutputStream(subset));
			ObjectInputStream sorted;
			ArrayList<Batch> blk;
			int b, e;
			int index;

			if (setName.equals("rightSubset")) {
				blk = blockRight;
				sorted = rightSorted;
				b = rB;
				e = rE;
				index = rightindex;
			} else {
				blk = blockLeft;
				sorted = leftSorted;
				b = lB;
				e = lE;
				index = leftindex;
			}

			int comp = 0;

			while (comp == 0) {
				// write current page until the last page in the current block
				// into subset file
				for (int i = b; i < blk.size(); i++) {
					sub.writeObject((Batch) blk.get(i));
				}
				blk = new ArrayList<Batch>();
				for (int i = 0; i < halfBlock; i++) {// read in another block
														// from sorted file
					if (sorted.available() != 0) {
						blk.add((Batch) sorted.readObject());
					} else {
						break;
					}
				}
				Tuple t2 = blk.get(blk.size() - 1) // get last element in last
													// page of block
						.elementAt(blk.get(halfBlock - 1).size() - 1);

				comp = Tuple.compareTuples(t, t2, index);
			}

			int offsetEqualB = -1;
			int offsetDiffB = -1;
			int offsetEqualE = -1;
			int offsetDiffE = -1;
			outerloop: for (int i = 0; i < blk.size(); i++) {
				for (int j = 0; j < blk.get(i).size(); j++) {
					Tuple t3 = blk.get(i).elementAt(j);
					int compRI = Tuple.compareTuples(t, t3, index);

					if (compRI == 0) {
						offsetEqualB = i;
						offsetEqualE = j;
					} else {
						offsetDiffB = i;
						offsetDiffE = j;
						break outerloop;
					}
				}
			}
			// write the rest pages containing same tuple into subset file
			for (int i = 0; i <= offsetEqualB; i++) {
				sub.writeObject((Batch) blk.get(i));
			}

			if (setName.equals("rightSubset")) {
				rB = offsetDiffB;
				rE = offsetDiffE;
			} else {
				lB = offsetDiffB;
				lE = offsetDiffE;
			}

		} catch (ClassNotFoundException error) {
			System.out.println("error in forming from subset: " + error);
		} catch (IOException io) {
			System.out.println("error in forming from subset: " + io);
		}

	}

	/** Close the operator */
	public boolean close() {
		File f = new File(rfname);
		f.delete();
		return true;
	}

	public int externalSort(Operator node, String nameOfFile, int indexOfAttr)
			throws ClassNotFoundException {
		Batch unsortedRun;
		int passCount = 0;
		int numOfRun = 0;
		String outSortedFile = "merged" + nameOfFile + "_pass0";
		try {
			ObjectOutputStream outSorted = new ObjectOutputStream(
					new FileOutputStream(outSortedFile));
			///// output sorted runs to object file
			// if(nameOfFile.equals("left")){
			while ((unsortedRun = (Batch) node.next()) != null) {
				Batch sortedRun = internalSort(unsortedRun, indexOfAttr);
				outSorted.writeObject(sortedRun);
				numOfRun++;
			}
			// }else{
			// }
			///// read in from object file to merge sorted runs
			ObjectInputStream inMerged = null;// initialize source file for
												// input runs
			int sizeOfOutputBatch = Batch.getPageSize()
					/ node.schema.getTupleSize(); // size of output page
			ArrayList<Integer> subRoundFileIndex = findNumOfPass(numOfRun);// subRound
																			// means
																			// num
																			// of
																			// blocks
																			// that
																			// one
																			// pass
																			// requires
			passCount = subRoundFileIndex.size();
			int curPass = 0;

			while (curPass < passCount) {

				//// determine the source file of unmerged runs for the initial
				//// pass
				if (curPass == 0) {
					inMerged = new ObjectInputStream(
							new FileInputStream(outSortedFile));
				}

				//// repeatedly read sorted runs into block buffer
				for (int subRound = 0; subRound < subRoundFileIndex
						.get(curPass); subRound++) {

					//// create the destination of merged runs, one block of
					//// pages form one file

					int outPassIndex = curPass + 1;
					int outSubRoundIndex = subRound;
					String partialOutMergedFile = "merged" + nameOfFile
							+ "_pass" + outPassIndex + "_subRound"
							+ outSubRoundIndex;
					;
					ObjectOutputStream partialOutMerged = new ObjectOutputStream(
							new FileOutputStream(partialOutMergedFile));

					//// for each page in each block, read tuples to the page
					//// until all tuples read

					if (curPass > 0) { // other than the initial pass, the input
										// source are multiple files
						ObjectInputStream[] partialInMerged = new ObjectInputStream[numBuff
								- 1];// give each page a pointer to respect
										// input file
						int validPage = 0; // count number of non-empty page in
											// the block
						int pageNo;
						for (pageNo = 0; pageNo < (numBuff - 1); pageNo++) {
							int inPassIndex = curPass;
							int inSubRoundIndex = subRound + pageNo;
							String partialInMergedFile = "merged" + nameOfFile
									+ "_pass" + inPassIndex + "_subRound"
									+ inSubRoundIndex;
							File f = new File(partialInMergedFile);
							if (!f.exists()) {
								break;
							} else {
								validPage++;
								partialInMerged[pageNo] = new ObjectInputStream(
										new FileInputStream(
												partialInMergedFile));
							}
						}

						boolean runHasMoreTuple = true; // flag to show there is
														// more tuple not read
														// into a page
						while (runHasMoreTuple) {
							block = new ArrayList<Batch>();// represents the
															// (numBuff-1) pages
															// storing sorted
															// runs for merge
							runHasMoreTuple = false;
							for (int i = 0; i < validPage; i++) {
								if (partialInMerged[i].available() != 0) {
									runHasMoreTuple = true;
									block.add((Batch) partialInMerged[i]
											.readObject());
								}
							}
							if (runHasMoreTuple) {
								mergeOneBlock(node, nameOfFile, indexOfAttr,
										partialOutMerged, sizeOfOutputBatch);
							}
						}
					} else { // if it is the first pass, read only from one file
						block = new ArrayList<Batch>(); // represents the
														// (numBuff-1) pages
														// storing sorted runs
														// for merge
						for (int i = 0; i < (numBuff - 1); i++) {
							block.add((Batch) inMerged.readObject());
						}
						mergeOneBlock(node, nameOfFile, indexOfAttr,
								partialOutMerged, sizeOfOutputBatch);
					}

				}
				//// increment number of pass
				curPass++;

			}

		} catch (IOException io) {
			System.out.println("error in outputting sorted file: " + io);
		}

		return passCount;
	}

	private void mergeOneBlock(Operator node, String nameOfFile,
			int indexOfAttr, ObjectOutputStream partialOutMerged,
			int sizeOfOutputBatch) {

		Batch outputBatch = new Batch(sizeOfOutputBatch); // the output page for
															// merged sorted
															// runs
		boolean blockHasMoreTuple = true;
		Tuple min = block.get(0).elementAt(0);
		Object minVal = block.get(0).elementAt(0).dataAt(indexOfAttr);
		;

		// sort the block buffer
		while (blockHasMoreTuple) {
			blockHasMoreTuple = false;
			int index = -1;
			for (int k = 0; k < (numBuff - 1); k++) { // get a temporary min
				if (block.get(k) != null) {
					min = block.get(k).elementAt(0);
					minVal = block.get(k).elementAt(0).dataAt(indexOfAttr);
				}
			}
			for (int j = 0; j < (numBuff - 1); j++) { // get the minimum among
														// current element(0)
				if (block.get(j) != null) {
					blockHasMoreTuple = true;
					Tuple toCheck = block.get(j).elementAt(0);
					Object toCheckVal = block.get(j).elementAt(0)
							.dataAt(indexOfAttr);
					if (compareElement(toCheckVal, minVal) < 0) { // if current
																	// element
																	// is
																	// smaller
																	// than min
						min = toCheck;
						index = j;
					}
				}
			}
			if (outputBatch.size() == sizeOfOutputBatch
			/* ||(outputBatch.size()>0|| !blockHasMoreTuple) */) {
				try {
					partialOutMerged.writeObject(outputBatch);
				} catch (IOException io) {
					System.out.println(
							"error in writing outputbatch object: " + io);
				}
				outputBatch = new Batch(sizeOfOutputBatch);
			} else if (blockHasMoreTuple) {
				outputBatch.add(min);
				block.get(index).remove(0); // remove current min from the block
											// buffer
			}
		}
	}

	private int compareElement(Object data1, Object data2) {
		if (data1 instanceof Integer) {
			return ((Integer) data1).compareTo((Integer) data2);
		} else if (data1 instanceof String) {
			return ((String) data1).compareTo((String) data2);

		} else if (data1 instanceof Float) {
			return ((Float) data1).compareTo((Float) data2);
		} else {
			System.out.println("InternalSort: Unknown comparision of the data");
			System.exit(1);
			return 0;
		}
	}

	private Batch internalSort(Batch run, int indexOfAttr) { // bubble sort
		boolean flag = true; // set flag to true to begin first pass
		Tuple temp; // holding variable

		while (flag) {
			flag = false; // set flag to false awaiting a possible swap
			for (int r = 0; r < run.size(); r++) {
				if (compareElement(run.elementAt(r).dataAt(indexOfAttr),
						run.elementAt(r + 1).dataAt(indexOfAttr)) > 0) // ascending
																		// sort
				{
					temp = run.elementAt(r); // swap elements
					run.remove(r);
					run.insertElementAt(temp, r + 1);
					flag = true; // shows a swap occurred
				}
			}
		}
		return run;
	}

	private ArrayList<Integer> findNumOfPass(int numOfRun) {
		ArrayList<Integer> numBlk = new ArrayList<Integer>();
		int count = 0;
		int base = numBuff - 1;
		while (numOfRun > 1) {
			count++;
			numOfRun = (int) Math.ceil(numOfRun / base);
			numBlk.add(numOfRun);
		}

		return numBlk;
	}

}
