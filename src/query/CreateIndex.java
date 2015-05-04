package query;

import heap.HeapFile;
import heap.HeapScan;
import index.HashIndex;
import global.Minibase;
import global.RID;
import global.SearchKey;
import parser.AST_CreateIndex;
import relop.FileScan;
import relop.Schema;
import relop.Tuple;


/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

	
	private String indexFile,indexTable,IndexColomn;
	private Schema schema;
	
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {
	  
	  
	  indexFile   = tree.getFileName();
	  indexTable  = tree.getIxTable();
	  IndexColomn = tree.getIxColumn();
	  
	  QueryCheck.tableExists(indexTable);
	  QueryCheck.fileNotExists(indexFile);
	  schema = Minibase.SystemCatalog.getSchema(indexTable);
	  QueryCheck.columnExists(schema, IndexColomn);
	  
	  
	  //check if and index already exists
	  int fldnos[]  = new int[1];
	  fldnos[0] = schema.fieldNumber(indexFile);
	  IndexDesc alreadyExists[] = Minibase.SystemCatalog.
			  getIndexes(indexTable, schema, fldnos);
	  
	  if(alreadyExists != null && alreadyExists.length != 0) {
		  throw new QueryException("index of "+indexFile+" already exists !");
	  }
	 

  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	  
	  	System.out.println(indexFile);
	    HashIndex hashidx = new HashIndex(indexFile);
	    HeapFile table = new HeapFile(indexTable);
		
		HeapScan scan = table.openScan(); 
		RID rid = new RID();
		
		while(scan.hasNext()){
			Tuple t = new Tuple(schema, scan.getNext(rid));
			hashidx.insertEntry(new SearchKey(t.getField(IndexColomn)), rid);
		}
		
		scan.close();
	
		Minibase.SystemCatalog.createIndex(indexFile, indexTable, IndexColomn);

  } // public void execute()

} // class CreateIndex implements Plan




