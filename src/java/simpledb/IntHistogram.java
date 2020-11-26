package simpledb;

import java.util.HashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram {

    private int buckets;
    private int minValue;
    private int maxValue;
    private int count;
    private int width;
    private HashMap<Integer,Integer> bucket;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.minValue = min;
        this.maxValue = max;
        this.count = 0;
        this.width = (int)Math.ceil((maxValue-minValue+1)*1.0/buckets);
        this.bucket = new HashMap<>();
        for(int i = 0;i<buckets;i++){
            bucket.put(i,0);
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(Object v) {
    	// some code goes here
        int index = getBucketIndex((int)v);
        bucket.put(index,bucket.get(index)+1);
        count++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, Object v) {
        // some code goes here
        int value = (int)v;
        int index = getBucketIndex(value);
        double total;
        switch (op){
            case EQUALS:
                if(value<minValue || value>maxValue){
                    return 0.0;
                }
                return bucket.get(index)*1.0/width/count;
            case LESS_THAN:
                if(value<minValue){
                    return 0.0;
                }
                if(value>maxValue){
                    return 1.0;
                }
                total = (getBucketOffset(value))*1.0/width*bucket.get(index);
                for(int i=0;i<index;i++){
                    total += bucket.get(i);
                }
                //System.out.println("val: "+total/cnt);
                return total/count;
            case GREATER_THAN:
                if(value<minValue){
                    return 1.0;
                }
                if(value>maxValue){
                    return 0.0;
                }
                //System.out.println("GREATER_THAN: "+hist+","+hist.get(index)+","+v);
                total = (width-1-getBucketOffset(value))*1.0/width*bucket.get(index);
                for(int i=index+1;i<buckets;i++){
                    total += bucket.get(i);
                }
                //System.out.println("val: "+total/cnt);
                return total/count;
            case NOT_EQUALS:
                return 1-estimateSelectivity(Predicate.Op.EQUALS,v);
            case LESS_THAN_OR_EQ:
                return 1-estimateSelectivity(Predicate.Op.GREATER_THAN,v);
            case GREATER_THAN_OR_EQ:
                return 1-estimateSelectivity(Predicate.Op.LESS_THAN,v);
        }
        return 0.0;
    }

    private int getBucketIndex(int v){
        int index = (int) Math.ceil((v-minValue)/width);
        if(index >= buckets){
            return buckets-1;
        }else {
            return index;
        }
    }

    private int getBucketOffset(int v){
        int offset = (v-minValue+1)%(int)width;
        if(offset == 0){
            return (int)width-1;
        }else {
            return offset-1;
        }
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return bucket.toString();
    }
}
