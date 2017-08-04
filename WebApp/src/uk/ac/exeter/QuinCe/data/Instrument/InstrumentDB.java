package uk.ac.exeter.QuinCe.data.Instrument;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;

import javax.sql.DataSource;

import javafx.animation.KeyValue.Type;
import uk.ac.exeter.QuinCe.User.User;
import uk.ac.exeter.QuinCe.data.Files.DataFileDB;
import uk.ac.exeter.QuinCe.data.Instrument.DataFormats.DateTimeColumnAssignment;
import uk.ac.exeter.QuinCe.data.Instrument.DataFormats.DateTimeSpecification;
import uk.ac.exeter.QuinCe.data.Instrument.DataFormats.LatitudeSpecification;
import uk.ac.exeter.QuinCe.data.Instrument.DataFormats.LongitudeSpecification;
import uk.ac.exeter.QuinCe.data.Instrument.DataFormats.PositionSpecification;
import uk.ac.exeter.QuinCe.utils.DatabaseException;
import uk.ac.exeter.QuinCe.utils.DatabaseUtils;
import uk.ac.exeter.QuinCe.utils.MissingParam;
import uk.ac.exeter.QuinCe.utils.MissingParamException;
import uk.ac.exeter.QuinCe.utils.RecordNotFoundException;

/**
 * Database methods dealing with instruments
 * @author Steve Jones
 *
 */
public class InstrumentDB {

	////////// *** CONSTANTS *** ///////////////
	
	/**
	 * Statement for inserting an instrument record
	 */
	private static final String CREATE_INSTRUMENT_STATEMENT = "INSERT INTO instrument ("
			+ "owner, name," // 2
			+ "pre_flushing_time, post_flushing_time, minimum_water_flow) " // 5
			+ "VALUES (?, ?, ?, ?, ?)";
	
	/**
	 * Statement for inserting a file definition record
	 */
	private static final String CREATE_FILE_DEFINITION_STATEMENT = "INSERT INTO file_definition ("
			+ "instrument_id, description, column_separator, " // 3
			+ "header_type, header_lines, header_end_string, " // 6
			+ "column_header_rows, column_count, " // 8
			+ "primary_position_file, lon_format, lon_value_col, lon_hemisphere_col, " // 12
			+ "lat_format, lat_value_col, lat_hemisphere_col, " // 15
			+ "date_time_col, date_time_props, date_col, date_props, " // 19
			+ "hours_from_start_col, hours_from_start_props, " // 21
			+ "jday_time_col, jday_col, year_col, month_col, day_col, " // 26
			+ "time_col, time_props, hour_col, minute_col, second_col) " // 31
			+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	
	
	/**
	 * Statement for retrieving all the details of a specific instrument
	 */
	private static final String GET_INSTRUMENT_QUERY = "SELECT owner, name, "
			+ "intake_temp_1_name, intake_temp_2_name, intake_temp_3_name, "
			+ "salinity_1_name, salinity_2_name, salinity_3_name, "
			+ "eqt_1_name, eqt_2_name, eqt_3_name, "
			+ "eqp_1_name, eqp_2_name, eqp_3_name, "
			+ "air_flow_1_name, air_flow_2_name, air_flow_3_name, "
			+ "water_flow_1_name, water_flow_2_name, water_flow_3_name, "
			+ "separator_char, date_format, time_format, " 
			+ "custom_datetime_format, custom_datetime_format_string, "
			+ "lat_format, lon_format, header_lines, has_atmospheric_pressure, samples_dried, "
			+ "run_type_col, date_col, year_col, month_col, day_col, "
			+ "time_col, hour_col, minute_col, second_col, custom_datetime_col, "
			+ "latitude_col, north_south_col, longitude_col, east_west_col, "
			+ "intake_temp_1_col, intake_temp_2_col, intake_temp_3_col, "
			+ "salinity_1_col, salinity_2_col, salinity_3_col, "
			+ "eqt_1_col, eqt_2_col, eqt_3_col, "
			+ "eqp_1_col, eqp_2_col, eqp_3_col, "
			+ "air_flow_1_col, air_flow_2_col, air_flow_3_col, "
			+ "water_flow_1_col, water_flow_2_col, water_flow_3_col, "
			+ "atmospheric_pressure_col, xh2o_col, co2_col, raw_col_count, "
			+ "pre_flushing_time, post_flushing_time "
			+ "FROM instrument WHERE id = ? ORDER BY name";
			
	
	/**
	 * Statement for inserting run types
	 */
	private static final String CREATE_RUN_TYPE_STATEMENT = "INSERT INTO run_types (instrument_id, run_name, run_type) VALUES (?, ?, ?)";
	
	/**
	 * Query for retrieving the list of instruments owned by a particular user
	 */
	private static final String GET_INSTRUMENT_LIST_QUERY = "SELECT id, name FROM instrument WHERE owner = ? ORDER BY name ASC";
	
	private static final String GET_RUN_TYPES_QUERY = "SELECT id, run_name, run_type FROM run_types WHERE instrument_id = ?";
	

	public static void storeInstrument(DataSource dataSource, Instrument instrument) throws MissingParamException, InstrumentException, DatabaseException, IOException {
		
		MissingParam.checkMissing(dataSource, "dataSource");
		MissingParam.checkMissing(instrument, "instrument");
		
		// Validate the instrument. Will throw an exception
		instrument.validate();
		
		Connection conn = null;
		PreparedStatement instrumentStatement = null;
		ResultSet instrumentKey = null;
		List<PreparedStatement> subStatements = new ArrayList<PreparedStatement>();
		List<ResultSet> fileDefinitionKeys = new ArrayList<ResultSet>();
		
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			
			// Create the instrument record
			instrumentStatement = makeCreateInstrumentStatement(conn, instrument);
			instrumentStatement.execute();
			instrumentKey = instrumentStatement.getGeneratedKeys();
			if (!instrumentKey.next()) {
				throw new DatabaseException("Instrument record was not created in the database");
			} else {
				long instrumentId = instrumentKey.getLong(1);
				instrument.setDatabaseId(instrumentId);
				
				// Now store the file definitions
				for (FileDefinition file : instrument.getFileDefinitions()) {
					System.out.println(CREATE_FILE_DEFINITION_STATEMENT);
					PreparedStatement fileStatement = makeCreateFileDefinitionStatement(conn, file, instrumentId);
					subStatements.add(fileStatement);
					fileStatement.execute();
					ResultSet fileKey = fileStatement.getGeneratedKeys();
					if (!fileKey.next()) {
						throw new DatabaseException("File Definition record was created in the database");
					} else {
						long fileDefinitionId = fileKey.getLong(1);
						
						// Sensor assignments
						
						
						
					}
					
				}
				
				
			}

			
			conn.commit();
		} catch (SQLException e) {
			boolean rollbackOK = true;
			
			try {
				conn.rollback();
			} catch (SQLException e2) {
				rollbackOK = false;
			}
			
			throw new DatabaseException("Exception while storing instrument", e, rollbackOK);
		} finally {
			DatabaseUtils.closeStatements(subStatements);
			DatabaseUtils.closeResultSets(instrumentKey);
			DatabaseUtils.closeStatements(instrumentStatement);
			DatabaseUtils.closeConnection(conn);
		}
	}

	/**
	 * Make the statement used to create an instrument record in the database
	 * @param conn A database connection
	 * @param instrument The instrument
	 * @return The database statement
	 * @throws SQLException If an error occurs while building the statement
	 */
	private static PreparedStatement makeCreateInstrumentStatement(Connection conn, Instrument instrument) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement(CREATE_INSTRUMENT_STATEMENT, Statement.RETURN_GENERATED_KEYS);
		stmt.setLong(1, instrument.getOwnerId()); // owner
		stmt.setString(2, instrument.getName()); // name
		stmt.setInt(3, instrument.getPreFlushingTime()); // pre_flushing_time
		stmt.setInt(4, instrument.getPostFlushingTime()); // post_flushing_time
		stmt.setInt(5, instrument.getMinimumWaterFlow()); // minimum_water_flow
		
		return stmt;
	}
	
	private static PreparedStatement makeCreateFileDefinitionStatement(Connection conn, FileDefinition file, long instrumentId) throws SQLException, IOException {
		
		PreparedStatement stmt = conn.prepareStatement(CREATE_FILE_DEFINITION_STATEMENT, Statement.RETURN_GENERATED_KEYS);
	
		stmt.setLong(1, instrumentId); // instrument_id
		stmt.setString(2, file.getFileDescription()); // description 
		stmt.setString(3, file.getSeparator()); // separator
		stmt.setInt(4, file.getHeaderType()); // header_type
		
		if (file.getHeaderType() == FileDefinition.HEADER_TYPE_LINE_COUNT) {
			stmt.setInt(5, file.getHeaderLines()); // header_lines
			stmt.setNull(6, Types.VARCHAR); // header_end_string
		} else {
			stmt.setNull(5, Types.INTEGER); // header_lines
			stmt.setString(6, file.getHeaderEndString()); // header_end_string
		}
		
		stmt.setInt(7, file.getColumnHeaderRows()); // column_header_rows
		stmt.setInt(8, file.getColumnCount()); // column_count
		stmt.setBoolean(9, file.getPositionPrimary()); // primary_position_file
		
		addPositionAssignment(stmt, file.getLongitudeSpecification(), 10, 11, 12); // longitude
		addPositionAssignment(stmt, file.getLatitudeSpecification(), 13, 14, 15); // latitude
		
		DateTimeSpecification dateTimeSpec = file.getDateTimeSpecification();
		addDateTimeAssignment(stmt, 16, 17, DateTimeSpecification.DATE_TIME, dateTimeSpec); // date_time_col
		addDateTimeAssignment(stmt, 18, 19, DateTimeSpecification.DATE, dateTimeSpec); // date
		addDateTimeAssignment(stmt, 20, 21, DateTimeSpecification.HOURS_FROM_START, dateTimeSpec); // hours_from_start
		addDateTimeAssignment(stmt, 22, -1, DateTimeSpecification.JDAY_TIME, dateTimeSpec); // jday_time_col
		addDateTimeAssignment(stmt, 23, -1, DateTimeSpecification.JDAY, dateTimeSpec); // jday_col
		addDateTimeAssignment(stmt, 24, -1, DateTimeSpecification.YEAR, dateTimeSpec); // year_col
		addDateTimeAssignment(stmt, 25, -1, DateTimeSpecification.MONTH, dateTimeSpec); // jmonth_col
		addDateTimeAssignment(stmt, 26, -1, DateTimeSpecification.DAY, dateTimeSpec); // jday_col
		addDateTimeAssignment(stmt, 27, 28, DateTimeSpecification.TIME, dateTimeSpec); // time_col
		addDateTimeAssignment(stmt, 29, -1, DateTimeSpecification.HOUR, dateTimeSpec); // hour_col
		addDateTimeAssignment(stmt, 30, -1, DateTimeSpecification.MINUTE, dateTimeSpec); // minute_col
		addDateTimeAssignment(stmt, 31, -1, DateTimeSpecification.SECOND, dateTimeSpec); // second_col
		
		return stmt;
	}
	
	private static void addPositionAssignment(PreparedStatement stmt, PositionSpecification posSpec, int formatIndex, int valueIndex, int hemisphereIndex) throws SQLException {
		stmt.setInt(formatIndex, posSpec.getFormat()); // lat_format
		stmt.setInt(valueIndex, posSpec.getValueColumn()); // lat_value_col
		
		if (posSpec.hemisphereRequired()) {
			stmt.setInt(hemisphereIndex, posSpec.getHemisphereColumn()); // lat_hemisphere_col
		} else {
			stmt.setNull(hemisphereIndex, Types.INTEGER); // lat_hemisphere_col
		}
	}
	
	/**
	 * Add a date/time column assignment to a statement for inserting a file definition
	 * @param stmt The file definition statement
	 * @param stmtColumnIndex The index in the statement to be set
	 * @param stmtPropsIndex The index in the statement to be set. If no properties are to be stored, set to -1
	 * @param assignmentId The required date/time assignment
	 * @param dateTimeSpec The file's date/time specification
	 * @throws SQLException If adding the assignment fails
	 * @throws IOException If the Properties object cannot be serialized into a String
	 */
	private static void addDateTimeAssignment(PreparedStatement stmt, int stmtColumnIndex, int stmtPropsIndex, int assignmentId, DateTimeSpecification dateTimeSpec) throws SQLException, IOException {
		
		int column = -1;
		Properties properties = null;
		
		DateTimeColumnAssignment assignment = dateTimeSpec.getAssignment(assignmentId);
		if (null != assignment) {
			if (assignment.isAssigned()) {
				column = assignment.getColumn();
				properties = assignment.getProperties();
			}
		}
		
		if (column == -1) {
			stmt.setInt(stmtColumnIndex, -1);
			if (stmtPropsIndex != -1) {
				stmt.setNull(stmtPropsIndex, Types.VARCHAR);
			}
		} else {
			stmt.setInt(stmtColumnIndex, column);

			if (null == properties && stmtPropsIndex != 1) {
				stmt.setNull(stmtPropsIndex, Types.VARCHAR);
			} else {
				StringWriter writer = new StringWriter();
				properties.store(writer, null);
				stmt.setString(stmtPropsIndex, writer.toString());
			}
		}
	}
	
	/**
	 * Returns a list of instruments owned by a given user.
	 * The list contains InstrumentStub objects, which just contain
	 * each instrument's ID and name.
	 * 
	 * The list is ordered by the name of the instrument.
	 * 
	 * @param owner The owner whose instruments are to be listed
	 * @return The list of instruments
	 * @throws MissingParamException If any required parameters are missing
	 * @throws DatabaseException If a database error occurred
	 */
	public static List<InstrumentStub> getInstrumentList(DataSource dataSource, User owner) throws MissingParamException, DatabaseException {
		
		MissingParam.checkMissing(dataSource, "dataSource");
		MissingParam.checkMissing(owner, "owner");
		
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet instruments = null;
		List<InstrumentStub> instrumentList = new ArrayList<InstrumentStub>();
		
		try {
			conn = dataSource.getConnection();
			stmt = conn.prepareStatement(GET_INSTRUMENT_LIST_QUERY);
			stmt.setLong(1, owner.getDatabaseID());
			
			instruments = stmt.executeQuery();
			while (instruments.next()) {
				InstrumentStub record = new InstrumentStub(instruments.getLong(1), instruments.getString(2));
				instrumentList.add(record);
			}
		} catch (SQLException e) {
			throw new DatabaseException("Error while retrieving instrument list", e);
		} finally {
			DatabaseUtils.closeResultSets(instruments);
			DatabaseUtils.closeStatements(stmt);
			DatabaseUtils.closeConnection(conn);
		}
		
		return instrumentList;
	}

	public static boolean instrumentExists(DataSource dataSource, User owner, String name) throws MissingParamException, DatabaseException {
		MissingParam.checkMissing(dataSource, "dataSource");
		MissingParam.checkMissing(owner, "owner");
		MissingParam.checkMissing(name, "name");
		
		boolean exists = false;
		
		for (InstrumentStub instrument : getInstrumentList(dataSource, owner)) {
			if (instrument.getName().equalsIgnoreCase(name)) {
				exists = true;
				break;
			}
		}
		
		return exists;
	}
	
	public static Instrument getInstrument(DataSource dataSource, long instrumentID) throws DatabaseException, MissingParamException, RecordNotFoundException {
		
		MissingParam.checkMissing(dataSource, "dataSource");
		MissingParam.checkPositive(instrumentID, "instrumentID");
		
		Connection conn = null;
		
		try {
			conn = dataSource.getConnection();
			return getInstrument(conn, instrumentID);
		} catch (SQLException e) {
			throw new DatabaseException("Error while updating record counts", e);
		} finally {
			DatabaseUtils.closeConnection(conn);
		}
	}

	
	/**
	 * Returns a complete instrument object for the specified instrument ID
	 * @param dataSource A data source
	 * @param instrumentID The instrument ID
	 * @return The complete Instrument object
	 * @throws MissingParamException If the data source is not supplied
	 * @throws DatabaseException If an error occurs while retrieving the instrument details
	 * @throws RecordNotFoundException If the specified instrument cannot be found
	 */
	public static Instrument getInstrument(Connection conn, long instrumentID) throws MissingParamException, DatabaseException, RecordNotFoundException {
		return null;
		/*
		
		MissingParam.checkMissing(conn, "conn");
		
		PreparedStatement instrStmt = null;
		ResultSet record = null;
		PreparedStatement runTypeStmt = null;
		ResultSet runTypeRecords = null;
		Instrument instrument = null;
		TreeSet<RunType> runTypes = new TreeSet<RunType>();
		
		try {
			instrStmt = conn.prepareStatement(GET_INSTRUMENT_QUERY);
			instrStmt.setLong(1, instrumentID);
			
			record = instrStmt.executeQuery();
			if (!record.next()) {
				throw new RecordNotFoundException("Instrument with id " + instrumentID + " does not exist");
			} else {
				instrument = new Instrument(record.getLong(1));
				instrument.setDatabaseId(instrumentID);
				instrument.setName(record.getString(2));
				instrument.setIntakeTempName1(record.getString(3));
				instrument.setIntakeTempName2(record.getString(4));
				instrument.setIntakeTempName3(record.getString(5));
				instrument.setSalinityName1(record.getString(6));
				instrument.setSalinityName2(record.getString(7));
				instrument.setSalinityName3(record.getString(8));
				instrument.setEqtName1(record.getString(9));
				instrument.setEqtName2(record.getString(10));
				instrument.setEqtName3(record.getString(11));
				instrument.setEqpName1(record.getString(12));
				instrument.setEqpName2(record.getString(13));
				instrument.setEqpName3(record.getString(14));
				instrument.setAirFlowName1(record.getString(15));
				instrument.setAirFlowName2(record.getString(16));
				instrument.setAirFlowName3(record.getString(17));
				instrument.setWaterFlowName1(record.getString(18));
				instrument.setWaterFlowName2(record.getString(19));
				instrument.setWaterFlowName3(record.getString(20));
				instrument.setSeparatorChar(record.getString(21).toCharArray()[0]);
				instrument.setDateFormat(record.getInt(22));
				instrument.setTimeFormat(record.getInt(23));
				instrument.setCustomDateTimeFormat(record.getBoolean(24));
				instrument.setCustomDateTimeFormatString(record.getString(25));
				instrument.setLatFormat(record.getInt(26));
				instrument.setLonFormat(record.getInt(27));
				instrument.setHeaderLines(record.getInt(28));
				instrument.setHasAtmosphericPressure(record.getBoolean(29));
				instrument.setSamplesDried(record.getBoolean(30));
				instrument.setColumnAssignment(Instrument.COL_RUN_TYPE, record.getInt(31));
				instrument.setColumnAssignment(Instrument.COL_DATE, record.getInt(32));
				instrument.setColumnAssignment(Instrument.COL_YEAR, record.getInt(33));
				instrument.setColumnAssignment(Instrument.COL_MONTH, record.getInt(34));
				instrument.setColumnAssignment(Instrument.COL_DAY, record.getInt(35));
				instrument.setColumnAssignment(Instrument.COL_TIME, record.getInt(36));
				instrument.setColumnAssignment(Instrument.COL_HOUR, record.getInt(37));
				instrument.setColumnAssignment(Instrument.COL_MINUTE, record.getInt(38));
				instrument.setColumnAssignment(Instrument.COL_SECOND, record.getInt(39));
				instrument.setColumnAssignment(Instrument.COL_CUSTOM_DATETIME_FORMAT, record.getInt(40));
				instrument.setColumnAssignment(Instrument.COL_LATITUDE, record.getInt(41));
				instrument.setColumnAssignment(Instrument.COL_NORTH_SOUTH, record.getInt(42));
				instrument.setColumnAssignment(Instrument.COL_LONGITUDE, record.getInt(43));
				instrument.setColumnAssignment(Instrument.COL_EAST_WEST, record.getInt(44));
				instrument.setColumnAssignment(Instrument.COL_INTAKE_TEMP_1, record.getInt(45));
				instrument.setColumnAssignment(Instrument.COL_INTAKE_TEMP_2, record.getInt(46));
				instrument.setColumnAssignment(Instrument.COL_INTAKE_TEMP_3, record.getInt(47));
				instrument.setColumnAssignment(Instrument.COL_SALINITY_1, record.getInt(48));
				instrument.setColumnAssignment(Instrument.COL_SALINITY_2, record.getInt(49));
				instrument.setColumnAssignment(Instrument.COL_SALINITY_3, record.getInt(50));
				instrument.setColumnAssignment(Instrument.COL_EQT_1, record.getInt(51));
				instrument.setColumnAssignment(Instrument.COL_EQT_2, record.getInt(52));
				instrument.setColumnAssignment(Instrument.COL_EQT_3, record.getInt(53));
				instrument.setColumnAssignment(Instrument.COL_EQP_1, record.getInt(54));
				instrument.setColumnAssignment(Instrument.COL_EQP_2, record.getInt(55));
				instrument.setColumnAssignment(Instrument.COL_EQP_3, record.getInt(56));
				instrument.setColumnAssignment(Instrument.COL_AIR_FLOW_1, record.getInt(57));
				instrument.setColumnAssignment(Instrument.COL_AIR_FLOW_2, record.getInt(58));
				instrument.setColumnAssignment(Instrument.COL_AIR_FLOW_3, record.getInt(59));
				instrument.setColumnAssignment(Instrument.COL_WATER_FLOW_1, record.getInt(60));
				instrument.setColumnAssignment(Instrument.COL_WATER_FLOW_2, record.getInt(61));
				instrument.setColumnAssignment(Instrument.COL_WATER_FLOW_3, record.getInt(62));
				instrument.setColumnAssignment(Instrument.COL_ATMOSPHERIC_PRESSURE, record.getInt(63));
				instrument.setColumnAssignment(Instrument.COL_XH2O, record.getInt(64));
				instrument.setColumnAssignment(Instrument.COL_CO2, record.getInt(65));
				instrument.setRawFileColumnCount(record.getInt(66));
				instrument.setPreFlushingTime(record.getInt(67));
				instrument.setPostFlushingTime(record.getInt(68));
				
				runTypeStmt = conn.prepareStatement(GET_RUN_TYPES_QUERY);
				runTypeStmt.setLong(1, instrumentID);
				runTypeRecords = runTypeStmt.executeQuery();
				while (runTypeRecords.next()) {
					runTypes.add(new RunType(runTypeRecords.getLong(1), instrumentID, runTypeRecords.getString(2), runTypeRecords.getInt(3)));
				}
				
				instrument.setRunTypes(runTypes);
			}
			
			
			
		} catch (SQLException e) {
			throw new DatabaseException("Error while retrieving instrument details", e);
		} finally {
			DatabaseUtils.closeResultSets(record, runTypeRecords);
			DatabaseUtils.closeStatements(instrStmt, runTypeStmt);
		}
		
		return instrument;
		
		*/
	}
	
	/**
	 * Get the Instrument object associated with a give data file,
	 * identified by its database ID
	 * @param dataSource A data source
	 * @param fileId The data file ID
	 * @return The instrument object
	 * @throws MissingParamException If any parameters are missing
	 * @throws DatabaseException If an unexpected database error occurs
	 * @throws RecordNotFoundException If the file ID is not in the database
	 */
	public static Instrument getInstrumentByFileId(DataSource dataSource, long fileId) throws MissingParamException, DatabaseException, RecordNotFoundException {
		
		MissingParam.checkMissing(dataSource, "dataSource");
		MissingParam.checkMissing(fileId, "fileId");
		
		long instrumentId = DataFileDB.getInstrumentId(dataSource, fileId);
		return getInstrument(dataSource, instrumentId);
	}
}
