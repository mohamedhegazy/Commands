package query;

import global.Minibase;
import index.HashIndex;
import parser.AST_DropIndex;

/**
 * Execution plan for dropping indexes.
 */
class DropIndex implements Plan {

	private String indexFile;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index doesn't exist
   */
  public DropIndex(AST_DropIndex tree) throws QueryException {
	  
	  indexFile   = tree.getFileName();
	  QueryCheck.indexExists(indexFile);

  } // public DropIndex(AST_DropIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
	  	HashIndex d = new HashIndex(indexFile);
	  	d.deleteFile();
		Minibase.SystemCatalog.dropIndex(indexFile);

		// print the output message
		System.out.println("Index dropped");

  } // public void execute()

} // class DropIndex implements Plan

