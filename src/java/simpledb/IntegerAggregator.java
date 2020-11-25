package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int groupByField;
    private Type groupByFieldType;
    private int aggregateField;
    private Op operator;
    private HashMap<Field,Integer> aggregateMap;
    private HashMap<Field,Integer> sizeMap;
    private TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.operator = what;
        this.aggregateMap = new HashMap<>();
        this.sizeMap = new HashMap<>();
        if(gbfield==NO_GROUPING){
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        }else{
            this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key;
        if(this.groupByField==NO_GROUPING){
            key = new IntField(0);
        }else{
            key = tup.getField(this.groupByField);
        }
        int value =((IntField)tup.getField(this.aggregateField)).getValue();

        if(aggregateMap.containsKey(key)){
            sizeMap.put(key,sizeMap.get(key)+1);
            Integer aggregateVal = aggregateMap.get(key);
            switch (this.operator){
                case MIN:
                    aggregateMap.put(key,Math.min(aggregateVal,value));
                    break;
                case MAX:
                    aggregateMap.put(key,Math.max(aggregateVal,value));
                    break;
                case SUM :
                case AVG:
                    aggregateMap.put(key,aggregateVal+value);
                    break;
                case COUNT:
                    aggregateMap.put(key,aggregateVal+1);
                    break;
            }
        }else {
            Integer init = null;
            switch (this.operator){
                case MIN:
                case SUM:
                case AVG:
                case MAX:
                    init = value;
                    break;
                case COUNT:
                    init = 1;
            }
            aggregateMap.putIfAbsent(key,init);
            sizeMap.putIfAbsent(key,1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator(){
            Iterator<Tuple> it;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                ArrayList<Tuple> result = new ArrayList<>();
                for(Map.Entry<Field,Integer> entry : aggregateMap.entrySet()){
                    Integer val = null;
                    switch (operator){
                        case MIN:
                        case SUM:
                        case COUNT:
                        case MAX:
                            val = entry.getValue();
                            break;
                        case AVG:
                            val = entry.getValue()/sizeMap.get(entry.getKey());
                    }
                    ArrayList<Field> current = new ArrayList();
                    if(groupByField == NO_GROUPING){
                        current.addAll(Arrays.asList(new Field[]{new IntField(val)}));
                        result.add(new Tuple(tupleDesc).setFields(current));
                    }else{
                        current.addAll(Arrays.asList(new Field[]{entry.getKey(), new IntField(val)}));
                        result.add(new Tuple(tupleDesc).setFields(current));
                    }
                }
                it = result.iterator();
            }
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return it.hasNext();
            }
            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return it.next();
            }
            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }
            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }
            @Override
            public void close() {
            }
        };
    }

}
