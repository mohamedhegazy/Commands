package query;

import index.HashIndex;
import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import parser.AST_Insert;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {
	  /** Name of the table to insert into. */
	  protected String fileName;

	  /** Schema of the table to insert into. */
	  protected Schema schema;
	  
	  /** Values to be inserted **/
	  protected Object []values;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exists or values are invalid
   */
  public Insert(AST_Insert tree) throws QueryException {
	  	fileName=tree.getFileName();
	  	schema=QueryCheck.tableExists(fileName);//check if table exists
	  	values=tree.getValues();//get values to be inserted
	  	QueryCheck.insertValues(schema, values);//check if values match table schema
  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	  Tuple new_tuple=new Tuple(schema,values);
	  HeapFile file=new HeapFile(fileName);
	  RID rid=new_tuple.insertIntoFile(file);
	  IndexDesc[] indexDescs=Minibase.SystemCatalog.getIndexes(fileName);//update hash indexes
	  for (int i = 0; i < indexDescs.length; i++) {
		HashIndex index=new HashIndex(indexDescs[i].indexName);//gets hash index file of the table
		SearchKey key=new SearchKey(new_tuple.getField(indexDescs[i].columnName));//hash the attribue value of the inserted 
																				//tuple for the current indexing column
		index.insertEntry(key, rid);
	}
    // print the output message
    System.out.println("Record Inserted");
    new_tuple.print();

  } // public void execute()

} // class Insert implements Plan
