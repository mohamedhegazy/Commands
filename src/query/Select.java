package query;
import global.Minibase;
import heap.HeapFile;
import java.util.ArrayList;
import java.util.Stack;
import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {
	/** Name of the tables to select from. */
	protected String[] filesName;
	/** Schema of the combined table. */
	protected Schema combined_schema;
	/** Columns in projection */
	protected String[] columns;
	/** Conditions of selection */
	protected Predicate[][] predicates;
	/**Iterator for each file **/
	protected Iterator []iterators;
	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if validation fails
	 */
	public Select(AST_Select tree) throws QueryException {	
		filesName = tree.getTables();
		Stack<Schema> eval = new Stack<>();
		for (int i = 0; i < filesName.length; i++) {
			eval.push(QueryCheck.tableExists(filesName[i]));
		}		
		while (!eval.empty()) {// combined schema
			Schema temp = eval.pop();
			if (!eval.empty()) {				
				Schema temp1 = Schema.join(temp, eval.pop());
				eval.push(temp1);
			} else {
				combined_schema = temp;
				break;
			}
		}
		columns = tree.getColumns();
		for (int i = 0; i < columns.length; i++) {// check each chosen column
													// does exist
			QueryCheck.columnExists(combined_schema, columns[i]);
		}
		predicates = tree.getPredicates();
		QueryCheck.predicates(combined_schema, predicates);// check correct form
															// of predicates
		// everything valid so we put plan
//		for(int i=0;i<predicates.length;i++,System.out.println())
//			for (int j = 0; j < predicates[i].length; j++) 
//				System.out.print(" "+predicates[i][j].toString());
		iterators=new Iterator[filesName.length];
		System.out.println("\n\n\n\n\n\n LINE !!!");
		for(int i=0;i<filesName.length;i++){
			//make an iterator for each table
			Schema schema=Minibase.SystemCatalog.getSchema(filesName[i]);
			iterators[i]=new FileScan(schema, new HeapFile(filesName[i]));
			//check if predicate belongs to one table (the row represents 
			//ORing of predicates so we want to know if the ORing is on 
			//the predicates of one table)
			ArrayList<Predicate[]> arr=new ArrayList<>();//hold predicates of the current table
			for(int j=0;j<predicates.length;j++){
				boolean valid=true;
				if(predicates[j]!=null){
					for (int k = 0; k < predicates[j].length; k++) {
						if(!predicates[j][k].validate(schema)){//means that predicate attributes don't belong to table
							valid=false;
							break;
						}
					}
					if(valid){
						arr.add(predicates[j]);
						predicates[j]=null;//means this predicate is not needed anymore;
					}	
				}
			}
				if(arr.size()>0)//means that we select certain tuples from table based on a condition
				{
					
					iterators[i]=new Selection(iterators[i], (Predicate[][])arr.toArray(new Predicate[arr.size()][]));
//					Predicate[][]test=(Predicate[][])arr.toArray(new Predicate[arr.size()][]);
//					System.out.println(filesName[i]+"  :");
//					for(int i1=0;i1<test.length;i1++,System.out.println()){
//						for(int j=0;j<test[i1].length;j++){
//							System.out.print(" "+test[i1][j].toString());
//						}
//					}
//
		}
			
		}					
	} // public Select(AST_Select tree) throws QueryException
	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		// print the output message
		Schema first=iterators[0].getSchema();//schema of first table that will be joined as we iterate tables
		Iterator result=iterators[0];//result table of joining and later projecting certain columns were chosen
		for (int i = 1; i < iterators.length; i++) {
			first=Schema.join(first, iterators[i].getSchema());
			ArrayList<Predicate[]>preds_of_join=new ArrayList<>();
			for(int j=0;j<predicates.length;j++){
				if(predicates[j]!=null){//not predicate of single table
					boolean valid = true;
					for (int k = 0; k < predicates[j].length; k++) {
							if(!predicates[j][k].validate(first)){//means predicate contains attributes not in joined schema
								valid=false;
								break;
							}
					}
					if(valid){
						preds_of_join.add(predicates[j]);
						predicates[j]=null;//means predicate belongs to joined tables so we won't consider it anymore
					}						
				}
			}
			result=new SimpleJoin(result, iterators[i], (Predicate[][])preds_of_join.toArray(new Predicate[preds_of_join.size()][]));			
		}//joining is done
		if(columns.length>0){//means we're projecting on certain columns
			Integer[] fields=new Integer[columns.length];
			for (int i = 0; i < fields.length; i++) {
				fields[i]=first.fieldNumber(columns[i]);	
			}			
			result=new Projection(result, fields);
		}
		int x=result.execute();
		result.close();
		System.out.println(x+ " rows returned.");
	} // public void execute()
} // class Select implements Plan
