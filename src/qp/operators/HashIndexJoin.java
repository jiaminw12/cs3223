package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class HashIndexJoin extends Join{


    int batchsize;  //Number of tuples per out batch

    /** The following fields are useful during execution of
     ** the NestedJoin operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    Batch outbatch;   // Output buffer
    Batch leftbatch;  // Buffer for left input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lcurs;    // Cursor for left side buffer
    int rcurs;    //Cursor for right side matching records
    boolean eosl;  // Whether end of stream (left table) is reached
    
    
    HashMap<Object, Vector<Tuple>> rightMap; // a hash map to store tuples under same value of an attr
    Vector<Tuple> valEqual; // to store vectors matches the given attr in hash map
    

    public HashIndexJoin(Join jn){
        super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    /** During open finds the index of the join attributes
     **  Materializes the right hand side
     **  Opens the connections
     **/

    public boolean open(){

        /** select number of tuples per batch **/
        int tuplesize=schema.getTupleSize();
        batchsize = Batch.getPageSize()/tuplesize;

        Attribute leftattr = con.getLhs();
        Attribute rightattr =(Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;
            
        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;
            
        /** 
         * read right table into the hash map
         **/
        if(!right.open()){
            return false;
        } else{    
            rightMap = new HashMap<Object, Vector<Tuple>>();
            while((rightpage = right.next()) != null){
                readIntoRightMap(rightpage);
            }
        }
        if(left.open())
            return true;
        else
            return false;
    }
    
    
    public void readIntoRightMap (Batch batch) {
    	Vector<Tuple> rows = batch.getTuples();
    	for (int i=0;i<rows.size();i++){
    		Object key = rows.get(i).dataAt(rightindex);
    		if(rightMap.get(key)==null){
    			Vector<Tuple> value = new Vector<Tuple>();
    			rightMap.put(key, value);
    			value.add(rows.get(i));
    		}else{
    			rightMap.get(key).add(rows.get(i));
    		}
    	}
    }


    /** from input buffers selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/
    public Batch next(){
    	// System.out.print("HashIndexJoin:--------------------------in
    	// next----------------");
    	// Debug.PPrint(con);
    	// System.out.println();
    	
    	int i,j,k;
        if(eosl && lcurs ==0){ 
            close();
            return null;
        }
        
        outbatch = new Batch(batchsize);
     
        while (!outbatch.isFull()){
        	
        	//// new left page is to be fetched
        	 if(lcurs==0){
                  if((leftbatch =(Batch) left.next()) == null){
                      eosl=true;
                      return outbatch;
                  }
              }
        	 
        
        	  
        	
            for (i = lcurs; i < leftbatch.size(); i++) {
                
        //// more tuples left in the old valEqual for join     
          	  if(rcurs > 0) 
                    {
                    	Tuple l = leftbatch.elementAt(lcurs);
                    	for (k = rcurs; k < valEqual.size(); k++) {
                            Tuple r = valEqual.elementAt(k);
                            if(l.checkJoin(r,leftindex,rightindex)) {
                                Tuple outtuple = l.joinWith(r);
                                outbatch.add(outtuple);                 
                                if(outbatch.isFull()){
                                	//case1:  more tuples left in valEqual 
                                    if(k < (valEqual.size()-1)){ 
                                    	rcurs = k+1;  
                                    } 
                                    //case2: all values in valEqual get joined
                                    else {
                                    	rcurs=0;
                                    	lcurs=(lcurs+1)%leftbatch.size();
                                    }
                                    return outbatch;
                                }
                            }                  
                        }
                        //
                        rcurs = 0;
                        lcurs=(lcurs+1)%leftbatch.size();
                        i = lcurs;
                    	
                    }
          	  
         //// a new valEqual that matches the current left tuple to be found
              
            	else	
            	{Tuple l = leftbatch.elementAt(i);
                	Object key = l.dataAt(leftindex);
                	 // filter those unmatched tuples
                    if ((valEqual= rightMap.get(key)) != null) {
                        for (j = 0; j < valEqual.size(); j++) {
                            Tuple r = valEqual.elementAt(j);
                            if(l.checkJoin(r,leftindex,rightindex)) {
                                Tuple outtuple = l.joinWith(r);
                                outbatch.add(outtuple);
                                if(outbatch.isFull()){
                                	//case 1: all tuples in valEqual get joined
                                    if (j == valEqual.size()-1){
                                    	
                                        lcurs = (i+1)%leftbatch.size();
                                        rcurs = 0;
                                        
                                    } 
                                    // case2: more tuples left in valEqual
                                    else { 
                                        lcurs = i;
                                        rcurs = j+1;
                                    }
                                    return outbatch;
                                }
                            }
                            
                        }
                        rcurs = 0;
                    }  
            	}
            }
            lcurs = 0;
        }
        return outbatch;
    }



    /** Close the operator */
    public boolean close(){
        return true;
    }
}