package uk.ac.exeter.QuinCe.jobs;

import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

import uk.ac.exeter.QuinCe.data.User;
import uk.ac.exeter.QuinCe.database.DatabaseException;
import uk.ac.exeter.QuinCe.database.User.NoSuchUserException;
import uk.ac.exeter.QuinCe.database.User.UserDB;
import uk.ac.exeter.QuinCe.utils.MissingDataException;
import uk.ac.exeter.QuinCe.utils.StringUtils;

/**
 * This class provides methods to manage the job queue
 * 
 * @author Steve Jones
 *
 */
public class JobManager {

	/**
	 * Indicates that a supplied job class passes all tests to ensure it is a valid job class
	 */
	protected static final int CLASS_CHECK_OK = 0;
	
	/**
	 * Indicates that a supplied job class cannot be found
	 */
	protected static final int CLASS_CHECK_NO_SUCH_CLASS = 1;
	
	/**
	 * Indicates that a supplied job class does not extend the root {@link Job} class
	 */
	protected static final int CLASS_CHECK_NOT_JOB_CLASS = 2;
	
	/**
	 * Indicates that the supplied job class does not have a valid constructor
	 */
	protected static final int CLASS_CHECK_INVALID_CONSTRUCTOR = 3;
	
	/**
	 * Indicates that a job has no owner
	 */
	private static final int NO_OWNER = -999;
	
	/**
	 * Indicates that a job record was not created in the database
	 */
	public static final int NOT_ADDED = -999;
	
	/**
	 * SQL statement to create a job record
	 */
	private static final String CREATE_JOB_STATEMENT = "INSERT INTO job (owner, submitted, class, parameters) VALUES (?, ?, ?, ?)";
	
	/**
	 * SQL statement to see if a job with a given ID exists
	 */
	private static final String FIND_JOB_QUERY = "SELECT COUNT(*) FROM job WHERE id = ?";
	
	private static final String SET_STATUS_STATEMENT = "UPDATE job SET status = ? WHERE id = ?";
	
	/**
	 * Adds a job to the database
	 * @param conn A database connection
	 * @param owner The job's owner (can be {@code null}
	 * @param jobClass The class name of the job to be run
	 * @param parameters The parameters of the job
	 * @throws DatabaseException If a database error occurs
	 * @throws MissingDataException Generated by internal checks - should never be thrown
	 * @throws NoSuchUserException If the supplied user does not exist in the database
	 * @throws JobClassNotFoundException If the specified job class does not exist
	 * @throws InvalidJobClassTypeException If the specified job class is not of the correct type
	 * @throws InvalidJobConstructorException If the specified job class does not have the correct constructor
	 * @throws JobException If an unknown problem is found with the specified job class
	 */
	public static long addJob(Connection conn, User owner, String jobClass, List<String> parameters) throws DatabaseException, MissingDataException, NoSuchUserException, JobClassNotFoundException, InvalidJobClassTypeException, InvalidJobConstructorException, JobException {
		
		long addedID = NOT_ADDED;
		
		// Get the user's ID
		int ownerID = NO_OWNER;

		if (null != owner) {
			// Check that the user exists
			if (null == UserDB.getUser(conn, owner.getEmailAddress())) {
				throw new NoSuchUserException(owner);
			}
			ownerID = owner.getDatabaseID();
		}
		
		// Check that the job class exists
		int classCheck = checkJobClass(jobClass);
		
		switch (classCheck) {
		case CLASS_CHECK_OK: {
			
			String paramsString = StringUtils.listToDelimited(parameters);
			Timestamp time = new Timestamp(System.currentTimeMillis());

			PreparedStatement stmt = null;

			try {
				stmt = conn.prepareStatement(CREATE_JOB_STATEMENT, Statement.RETURN_GENERATED_KEYS);
				if (NO_OWNER == ownerID) {
					stmt.setNull(1, java.sql.Types.INTEGER);
				} else {
					stmt.setInt(1, ownerID);
				}
				
				stmt.setTimestamp(2, time);
				stmt.setString(3, jobClass);
				stmt.setString(4, paramsString);
				
				stmt.execute();
				
				ResultSet generatedKeys = stmt.getGeneratedKeys();
				if (generatedKeys.next()) {
					addedID = generatedKeys.getLong(1);
				}
			} catch(SQLException e) {
				throw new DatabaseException("An error occurred while storing the job", e);
			} finally {
				if (null != stmt) {
					try {
						stmt.close();
					} catch (SQLException e) {
						// Do nothing
					}
				}
			}
			
			break;
		}
		case CLASS_CHECK_NO_SUCH_CLASS: {
			throw new JobClassNotFoundException(jobClass);
		}
		case CLASS_CHECK_NOT_JOB_CLASS: {
			throw new InvalidJobClassTypeException(jobClass);
		}
		case CLASS_CHECK_INVALID_CONSTRUCTOR: {
			throw new InvalidJobConstructorException(jobClass);
		}
		default: {
			throw new JobException("Unknown fault with job class '" + jobClass);
		}
		}
		
		return addedID;
	}

	/**
	 * Sets the status of a job
	 * @param conn A database connection
	 * @param jobID The ID of the job whose status is to be set
	 * @param status The status to be set
	 * @throws BadStatusException If the supplied status is invalid
	 * @throws NoSuchJobException If the specified job does not exist
	 * @throws DatabaseException If an error occurs while updating the database
	 */
	public static void setStatus(Connection conn, long jobID, String status) throws BadStatusException, NoSuchJobException, DatabaseException {
		if (!checkJobStatus(status)) {
			throw new BadStatusException(status);
		}
		
		if (!jobExists(conn, jobID)) {
			throw new NoSuchJobException(jobID);
		}
		
		PreparedStatement stmt = null;
		
		try {
			stmt = conn.prepareStatement(SET_STATUS_STATEMENT);
			stmt.setString(1, status);
			stmt.setLong(2, jobID);
			stmt.execute();
		} catch (SQLException e) {
			throw new DatabaseException("An error occurred while setting the status", e);
		} finally {
			if (null != stmt) {
				try {
					stmt.close();
				} catch (SQLException e) {
					// Do nothing.
				}
			}
		}
	}
	
	/**
	 * Determines whether or not a job with the given ID exists in the database
	 * @param conn A database connection
	 * @param jobID The job ID
	 * @return {@code true} if the job exists; {@code false} otherwise
	 * @throws DatabaseException If an error occurs while searching the database
	 */
	private static boolean jobExists(Connection conn, long jobID) throws DatabaseException {
		boolean jobExists = false;
		
		PreparedStatement stmt = null;
		
		try {
			stmt = conn.prepareStatement(FIND_JOB_QUERY);
			stmt.setLong(1, jobID);
			
			ResultSet result = stmt.executeQuery();
			if (result.next()) {
				if (result.getInt(1) > 0) {
					jobExists = true;
				}
			}
		} catch (SQLException e) {
			throw new DatabaseException("An error occurred while checking for a job's existence", e);
		} finally {
			if (null != stmt) {
				try {
					stmt.close();
				} catch (SQLException e) {
					// Do nothing
				}
			}
		}
		
		return jobExists;
	}
	
	/**
	 * Checks a class name to see if it a valid {@link Job} class
	 * @param jobClass The class name
	 * @return An integer flag containing the result of the check. See {@code CLASS_CHECK_*} fields.
	 */
	protected static int checkJobClass(String jobClass) {
		
		int checkResult = CLASS_CHECK_OK;
		
		try {
			Class<?> jobClazz = Class.forName(jobClass);
			
			// Does it inherit from the job class?
			if (!(jobClazz.getSuperclass().equals(Job.class))) {
				checkResult = CLASS_CHECK_NOT_JOB_CLASS;
			} else {
				// Is there a constructor that takes the right parameters?
				// We also check that the List is designated to contain String objects
				Constructor<?> jobConstructor = jobClazz.getConstructor(Connection.class, List.class);
				Type[] constructorGenericTypes = jobConstructor.getGenericParameterTypes();
				if (constructorGenericTypes.length != 2) {
					checkResult = CLASS_CHECK_INVALID_CONSTRUCTOR;
				} else {
					if (!(constructorGenericTypes[1] instanceof ParameterizedType)) {
						checkResult = CLASS_CHECK_INVALID_CONSTRUCTOR;
					} else {
						Type[] actualTypeArguments = ((ParameterizedType) constructorGenericTypes[1]).getActualTypeArguments();
						if (actualTypeArguments.length != 1) {
							checkResult = CLASS_CHECK_INVALID_CONSTRUCTOR;
						} else {
							Class<?> typeArgumentClass = (Class<?>) actualTypeArguments[0];
							if (!typeArgumentClass.equals(String.class)) {
								checkResult = CLASS_CHECK_INVALID_CONSTRUCTOR;
							}
						}
					}
				}
			}
		} catch (ClassNotFoundException e) {
			checkResult = CLASS_CHECK_NO_SUCH_CLASS;
		} catch (NoSuchMethodException e) {
			checkResult = CLASS_CHECK_INVALID_CONSTRUCTOR;
		}
		
		return checkResult;
	}
	
	/**
	 * Checks a job status string to make sure it's valid
	 * @param status The status string to be checked
	 * @return {@code true} if the status string is valid; {@code false} otherwise
	 */
	private static boolean checkJobStatus(String status) {
		
		boolean statusOK = false;
		
		if (status.equals(Job.WAITING_STATUS) ||
				status.equals(Job.RUNNING_STATUS) ||
				status.equals(Job.FINISHED_STATUS) ||
				status.equals(Job.ERROR_STATUS)) {

			statusOK = true;
		}
		
		return statusOK;
	}
}
