package uk.ac.exeter.QuinCe.web;

import java.util.List;

/**
 * Bean for pages containing plots and tables
 * @author Steve Jones
 *
 */
public abstract class PlotPageBean extends BaseManagedBean {

	/**
	 * The data for the current view of the data table.
	 * 
	 * <p>
	 *   The table data is loaded on demand as the user scrolls around, to
	 *   eliminate the delay and memory requirements of loading a complete
	 *   voayge's data in one go. When the user scrolls to a particular row,
	 *   the data for that row and a set of rows before and after it is loaded.
	 * </p>
	 * 
	 * @see #generateTableData()
	 * @see <a href="https://datatables.net/examples/data_sources/server_side.html">DataTables Server-Side Processing</a>
	 */
	private String tableJsonData = null;

	/**
	 * The list of row ids in the data being displayed.
	 */
	private List<Long> tableRowIds = null;
	
	/**
	 * The list of row IDs as a JSON string
	 */
	private String tableRowIdsJson = null;
	
	/**
	 * The table headings as a JSON array
	 */
	private String tableHeadings;
		
 	/**
 	 * An internal value for the DataTables library,
 	 * used when drawing retrieving table data from the server
	 * @see <a href="https://datatables.net/examples/data_sources/server_side.html">DataTables Server-Side Processing</a>
 	 */
	private int tableDataDraw;		

	/**
	 * The first row of the table data view to be loaded for DataTables
	 * @see <a href="https://datatables.net/examples/data_sources/server_side.html">DataTables Server-Side Processing</a>
	 */
	private int tableDataStart;		

	/**
	 * The number of rows to be loaded for DataTables
	 * @see <a href="https://datatables.net/examples/data_sources/server_side.html">DataTables Server-Side Processing</a>
	 */
	private int tableDataLength;	
	
	/**
	 * A Javascript array string containing the list of all row numbers in the current data file that can be
	 * selected in the data table. Selectable rows can have their WOCE flag set by the user.
	 * Unselectable rows are typically rows that have their QC flag set to FATAL, which means they
	 * cannot be processed at all.
	 * 
	 * <p>
	 * The rows are loaded during {@link #start} via {@link #buildSelectableRowIds()}.
	 * </p>
	 */
	protected String selectableRowIds = null;

	/**
	 * The row numbers that have been selected by the user. Stored as a comma-separated list.
	 */
	private String selectedRows = null;
	
	/**
	 * The plot data as a JSON string
	 */
	protected String plotData;

	/**
	 * The labels for the plot
	 */
	protected String plotLabels;
	
	/**
	 * Get the data for the current view in the data table
	 * @return The table data
	 * @see #tableJsonData
	 */
	public String getTableJsonData() {
 		return tableJsonData;		
 	}
	
	/**
	 * Set the data for the current view in the data table
	 * @param tableJsonData The table data
	 * @see #tableJsonData
	 */
 	public void setTableJsonData(String tableJsonData) {
 		this.tableJsonData = tableJsonData;
 	}		
	
	/**
	 * Get the total number of rows in the data file. If the number of rows is not
	 * known, the result will be negative.
	 * 
	 * <p>
	 *   Note that this is the number
	 *   of atmospheric or ocean records, depending on what is being displayed.
	 * </p>
	 * 
	 * @return The number of records
	 */
	public int getRecordCount() {		
		int result = -1;
		if (null != tableRowIds) {
			result = tableRowIds.size();
		}
		
		return result;		
	}
	
	/**
	 * Set the record count (dummy method)
	 * @param recordCount Ignored.
	 */
	public void setRecordCount(int recordCount) {
		// Do nothing
	}
	
	/**
	 * Returns the list of table rows as a JSON string
	 * @return The table row IDs
	 */
	public String getTableRowIds() {
		String result = "[]";
		if (null != tableRowIdsJson) {
			result = tableRowIdsJson;
		}
		
		return result;
	}
 
	/**
	 * Set the table row IDs (dummy method)
	 * @param tableRowIds Ignored.
	 */
	public void setTableRowIds(String tableRowIds) {
		// Do nothing
	}
	
 	/**
 	 * Get the current value for the DataTables internal {@code draw} parameter
 	 * @return The DataTables {@code draw} parameter
 	 * @see #tableDataDraw
 	 */
 	public int getTableDataDraw() {		
		return tableDataDraw;		
	}		
			
 	/**
 	 * Set the value for the DataTables internal {@code draw} parameter
 	 * @param tableDataDraw The DataTables {@code draw} parameter
 	 * @see #tableDataDraw
 	 */
	public void setTableDataDraw(int tableDataDraw) {		
		this.tableDataDraw = tableDataDraw;		
	}		

	/**
	 * Get the first row of the current view in the data table
	 * @return The first row of the view
	 * @see #tableDataStart
	 */
	public int getTableDataStart() {		
		return tableDataStart;		
	}		

	/**
	 * Set the first row of the view in the data table
	 * @param tableDataStart The first row of the view
	 * @see #tableDataStart
	 */
	public void setTableDataStart(int tableDataStart) {		
		this.tableDataStart = tableDataStart;		
	}		
	
	/**
	 * Get the number of rows in the current view in the data table
	 * @return The number of rows in the view
	 * @see #tableDataLength
	 */
	public int getTableDataLength() {		
		return tableDataLength;		
	}		

	/**
	 * Set the number of rows in the current view in the data file
	 * @param tableDataLength The number of rows in the view
	 * @see #tableDataLength
	 */
	public void setTableDataLength(int tableDataLength) {		
		this.tableDataLength = tableDataLength;		
	}
	
	/**
	 * Get the list of selectable row IDs
	 * @return The selectable rows
	 */
	public String getSelectableRowIds() {
		return selectableRowIds;
	}
	
	/**
	 * Get the set of rows that have been selected by the user.
	 * The rows are returned as an unsorted comma-separated list.
	 * @return The selected rows
	 */
	public String getSelectedRows() {
		return selectedRows;
	}
	
	/**
	 * Set the selected table rows.
	 * @param selectedRows The selected rows
	 * @see #selectedRows
	 */
	public void setSelectedRows(String selectedRows) {
		this.selectedRows = selectedRows;
	}
	
	/**
	 * Retrieve the data for the table from the database as a JSON string.
	 * The data is stored in {@link #tableJsonData}.
	 */
	public void generateTableData() {
		try {
			if (null == tableRowIds) {
				tableRowIds = loadRowIds();
				
				StringBuilder json = new StringBuilder();
				json.append('[');
				for (int i = 0; i < tableRowIds.size(); i++) {
					json.append(tableRowIds.get(i));
					if (i < tableRowIds.size() - 1) {
						json.append(',');
					}
				}
				json.append(']');
				
				tableRowIdsJson = json.toString();
			}
			
			if (null == tableHeadings) {
				buildTableHeadings();
			}
			
			tableJsonData = loadTableData(tableDataStart, tableDataLength);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Clear existing table data.
	 * This will force calls to generateTableData to
	 * reinitialise everything.
	 */
	private void clearTableData() {
		tableRowIds = null;
		tableRowIdsJson = null;
		tableHeadings = null;
		tableJsonData = null;
	}
	
	/**
	 * Get the list of row IDs for the data being displayed
	 * @return The row IDs
	 */
	protected abstract List<Long> loadRowIds() throws Exception;

	/**
	 * Build the table headings
	 */
	protected abstract String buildTableHeadings();
	
	/**
	 * Get the headings for the table
	 * @return The table headings
	 */
	public String getTableHeadings() {
		return tableHeadings;
	}

	/**
	 * Get the plot data
	 * @return The plot data
	 */
	public String getPlotData() {
		return plotData;
	}
	
	/**
	 * Dummy method to set the plot data. Does nothing
	 * @param plotData The supplied data; ignored
	 */
	public void setPlotData(String plotData) {
		// Do nothing
	}
	
	/**
	 * Get the labels for the plot
	 * @return The plot labels
	 */
	public String getPlotLabels() {
		return plotLabels;
	}
	
	/**
	 * Set the labels for the plot (dummy)
	 * @param dummy The supplied labels. Ignored.
	 */
	public void setPlotLabels(String dummy) {
		// Do nothing
	}
	
	/**
	 * Reload all the data on the page
	 */
	public void reloadPageData() {
		reloadPlotData();
		clearTableData();
		generateTableData();
	}
	
	/**
	 * Reload the plot data. Can be used if the plot is changed.
	 */
	public void reloadPlotData() {
		try {
			plotLabels = buildPlotLabels();
			plotData = loadPlotData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Start a new bean instance
	 * @return Navigation to the plot page
	 */
	public String start() {
		try {
			init();
			plotLabels = buildPlotLabels();
			plotData = loadPlotData();
			tableHeadings = buildTableHeadings();
			selectableRowIds = buildSelectableRowIds();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return getScreenNavigation();
	}

	/**
	 * Perform bean-specific initialisation
	 */
	protected abstract void init();
	
	/**
	 * Get the navigation to the plot screen
	 * @return The navigation to the plot screen
	 */
	protected abstract String getScreenNavigation();
	
	/**
	 * Load the data for the first plot view
	 * @return The plot data
	 * @throws Exception If the data cannot be retrieved
	 */
	protected abstract String loadPlotData() throws Exception;
	
	/**
	 * Build the list of selectable record IDs
	 * @throws Exception If the list cannot be created
	 */
	protected abstract String buildSelectableRowIds() throws Exception;
	
	/**
	 * Build the labels for the plot
	 */
	protected abstract String buildPlotLabels();
	
	/**
	 * Load the data for the specified portion of the table as a JSON string
	 * @param start The first record to retrieve
	 * @param length The number of records to retrieve
	 * @return The table data
	 */
	protected abstract String loadTableData(int start, int length) throws Exception;
}
