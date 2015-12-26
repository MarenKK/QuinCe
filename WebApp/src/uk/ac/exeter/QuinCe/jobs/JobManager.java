package uk.ac.exeter.QuinCe.jobs;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import uk.ac.exeter.QuinCe.data.User;
import uk.ac.exeter.QuinCe.database.DatabaseException;
import uk.ac.exeter.QuinCe.database.DatabaseUtils;
import uk.ac.exeter.QuinCe.database.User.NoSuchUserException;
import uk.ac.exeter.QuinCe.database.User.UserDB;
import uk.ac.exeter.QuinCe.utils.MissingParam;
import uk.ac.exeter.QuinCe.utils.MissingParamException;
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
	 * SQL statement to create a job record
	 */
	private static final String CREATE_JOB_STATEMENT = "INSERT INTO job (owner, submitted, class, parameters) VALUES (?, ?, ?, ?)";
	
	/**
	 * SQL statement to see if a job with a given ID exists
	 */
	private static final String FIND_JOB_QUERY = "SELECT COUNT(*) FROM job WHERE id = ?";
	
	/**
	 * SQL statement for setting a job's status
	 */
	private static final String SET_STATUS_STATEMENT = "UPDATE job SET status = ? WHERE id = ?";
	
	/**
	 * SQL statement for setting a job's progress
	 */
	private static final String SET_PROGRESS_STATEMENT = "UPDATE job SET progress = ? WHERE id = ?";
	
	/**
	 * SQL statement for recording that a job has started
	 */
	private static final String START_JOB_STATEMENT = "UPDATE job SET status = '" + Job.RUNNING_STATUS + "', started = ? WHERE id = ?";
	
	/**
	 * SQL statement for recording that a job has completed
	 */
	private static final String END_JOB_STATEMENT = "UPDATE job SET status = '" + Job.FINISHED_STATUS + "', ended = ? WHERE id = ?";
	
	/**
	 * SQL statement for recording that a job has failed with an error
	 */
	private static final String ERROR_JOB_STATEMENT = "UPDATE job SET status = '" + Job.ERROR_STATUS + "', ended = ?, stack_trace = ? WHERE id = ?";
		
	/**
	 * SQL statement to retrieve a job's class and paremeters
	 */
	private static final String GET_JOB_QUERY = "SELECT id, class, parameters FROM job WHERE id = ?";
	
	/**
	 * SQL statement to retrieve the next queued job
	 */
	private static final String GET_NEXT_JOB_QUERY = "SELECT id, class, parameters FROM job ORDER BY submitted ASC LIMIT 1";
	
	/**
	 * Adds a job to the database
	 * @param conn A database connection
	 * @param owner The job's owner (can be {@code null}
	 * @param jobClass The class name of the job to be run
	 * @param parameters The parameters of the job
	 * @throws DatabaseException If a database error occurs
	 * @throws MissingParamException Generated by internal checks - should never be thrown
	 * @throws NoSuchUserException If the supplied user does not exist in the database
	 * @throws JobClassNotFoundException If the specified job class does not exist
	 * @throws InvalidJobClassTypeException If the specified job class is not of the correct type
	 * @throws InvalidJobConstructorException If the specified job class does not have the correct constructor
	 * @throws JobException If an unknown problem is found with the specified job class
	 */
	public static long addJob(DataSource dataSource, User owner, String jobClass, List<String> parameters) throws DatabaseException, MissingParamException, NoSuchUserException, JobClassNotFoundException, InvalidJobClassTypeException, InvalidJobConstructorException, JobException {
		
		MissingParam.checkMissing(dataSource, "dataSource");

		long addedID = DatabaseUtils.NO_DATABASE_RECORD;
		
		// Get the user's ID
		int ownerID = NO_OWNER;

		if (null != owner) {
			// Check that the user exists
			if (null == UserDB.getUser(dataSource, owner.getEmailAddress())) {
				throw new NoSuchUserException(owner);
			}
			ownerID = owner.getDatabaseID();
		}
		
		// Check that the job class exists
		int classCheck = checkJobClass(jobClass);
		
		switch (classCheck) {
		case CLASS_CHECK_OK: {
			
			Connection connection = null;
			PreparedStatement stmt = null;

			try {
				connection = dataSource.getConnection();
				stmt = connection.prepareStatement(CREATE_JOB_STATEMENT, Statement.RETURN_GENERATED_KEYS);
				if (NO_OWNER == ownerID) {
					stmt.setNull(1, java.sql.Types.INTEGER);
				} else {
					stmt.setInt(1, ownerID);
				}
				
				stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
				stmt.setString(3, jobClass);
				stmt.setString(4, StringUtils.listToDelimited(parameters));
				
				stmt.execute();
				
				ResultSet generatedKeys = stmt.getGeneratedKeys();
				if (generatedKeys.next()) {
					addedID = generatedKeys.getLong(1);
				}
			} catch(SQLException e) {
				throw new DatabaseException("An error occurred while storing the job", e);
			} finally {
				try {
					if (null != stmt) {
						stmt.close();
					}
					if (null != connection) {
						connection.close();
					}
				} catch (SQLException e) {
					// Do nothing
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
	 * Adds a job to the database, and instantly runs it
	 * @param conn A database connection
	 * @param owner The job's owner (can be {@code null}
	 * @param jobClass The class name of the job to be run
	 * @param parameters The parameters of the job
	 * @throws DatabaseException If a database error occurs
	 * @throws MissingParamException Generated by internal checks - should never be thrown
	 * @throws NoSuchUserException If the supplied user does not exist in the database
	 * @throws JobClassNotFoundException If the specified job class does not exist
	 * @throws InvalidJobClassTypeException If the specified job class is not of the correct type
	 * @throws InvalidJobConstructorException If the specified job class does not have the correct constructor
	 * @throws JobException If an unknown problem is found with the specified job class
	 * @throws JobThreadPoolNotInitialisedException If the job thread pool has not been initialised
	 * @throws NoSuchJobException If the job mystreriously vanishes between being created and run
	 */
	public static void addInstantJob(DataSource dataSource, Properties config, User owner, String jobClass, List<String> parameters) throws DatabaseException, MissingParamException, NoSuchUserException, JobClassNotFoundException, InvalidJobClassTypeException, InvalidJobConstructorException, JobException, JobThreadPoolNotInitialisedException, NoSuchJobException {
		long jobID = addJob(dataSource, owner, jobClass, parameters);
		JobThread emailThread = JobThreadPool.getInstance().getInstantJobThread(JobManager.getJob(dataSource, config, jobID));
		startJob(dataSource, jobID);
		emailThread.start();
	}

	/**
	 * Sets the status of a job
	 * @param conn A database connection
	 * @param jobID The ID of the job whose status is to be set
	 * @param status The status to be set
	 * @throws UnrecognisedStatusException If the supplied status is invalid
	 * @throws NoSuchJobException If the specified job does not exist
	 * @throws DatabaseException If an error occurs while updating the database
	 */
	public static void setStatus(DataSource dataSource, long jobID, String status) throws MissingParamException, UnrecognisedStatusException, NoSuchJobException, DatabaseException {

		MissingParam.checkMissing(dataSource, "dataSource");
		
		if (!checkJobStatus(status)) {
			throw new UnrecognisedStatusException(status);
		}
		
		if (!jobExists(dataSource, jobID)) {
			throw new NoSuchJobException(jobID);
		}
		
		Connection connection = null;
		PreparedStatement stmt = null;
		
		try {
			connection = dataSource.getConnection();
			stmt = connection.prepareStatement(SET_STATUS_STATEMENT);
			stmt.setString(1, status);
			stmt.setLong(2, jobID);
			stmt.execute();
		} catch (SQLException e) {
			throw new DatabaseException("An error occurred while setting the status", e);
		} finally {
			try {
				if (null != stmt) {
					stmt.close();
				}
				if (null != connection) {
					connection.close();
				}
			} catch (SQLException e) {
				// Do nothing.
			}
		}
	}
	
	/**
	 * Update a job record with the necessary details when it's started. The {@code status} is set to
	 * {@link Job.RUNNING_STATE}, and the {@code started} field is given the current time.
	 * @param conn A database connection
	 * @param jobID The job that has been started
	 * @throws DatabaseException If an error occurs while updating the record
	 * @throws NoSuchJobException If the specified job doesn't exist
	 */
	public static void startJob(DataSource dataSource, long jobID) throws MissingParamException, DatabaseException, NoSuchJobException {
		
		MissingParam.checkMissing(dataSource, "dataSource");
		
		if (!jobExists(dataSource, jobID)) {
			throw new NoSuchJobException(jobID);
		}

		Connection connection = null;
		PreparedStatement stmt = null;
		
		try {
			connection = dataSource.getConnection();
			stmt = connection.prepareStatement(START_JOB_STATEMENT);
			stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
			stmt.setLong(2, jobID);
			stmt.execute();
		} catch (SQLException e) {
			throw new DatabaseException("An error occurred while setting the job to 'started' state", e);
		} finally {
			try {
				if (null != stmt) {
					stmt.close();
				}
				if (null != connection) {
					connection.close();
				}
			} catch (SQLException e) {
				// Do nothing.
			}
		}
	}
	
	/**
	 * Retrieve a Job object from the database
	 * @param conn A database connection
	 * @param jobID The job ID
	 * @return A Job object that can be used to run the job
	 * @throws DatabaseException If any errors occurred retrieving the Job object
	 * @throws NoSuchJobException If the specified job does not exist
	 */
	public static Job getJob(DataSource dataSource, Properties config, long jobID) throws MissingParamException, DatabaseException, NoSuchJobException {
		
		MissingParam.checkMissing(dataSource, "dataSource");

		Job job = null;
		Connection connection = null;
		PreparedStatement stmt = null;
		
		try {
			connection = dataSource.getConnection();
			stmt = connection.prepareStatement(GET_JOB_QUERY);
			stmt.setLong(1, jobID);
			
			ResultSet result = stmt.executeQuery();
			if (!result.next()) {
				throw new NoSuchJobException(jobID);
			} else {
				job = getJobFromResultSet(result, dataSource, config);
			}
		} catch (SQLException|ClassNotFoundException|NoSuchMethodException|InstantiationException|IllegalAccessException|IllegalArgumentException|InvocationTargetException e) {
			// We handle all exceptions as DatabaseExceptions.
			// The fact is that invalid jobs should never get into the database in the first place.
			throw new DatabaseException("Error while retrieving details for job " + jobID, e);
		} finally {
			try {
				if (null != stmt) {
					stmt.close();
				}
				if (null != connection) {
					connection.close();
				}
			} catch (SQLException e) {
				// Do nothing.
			}
		}
		
		return job;
	}
	
	private static Job getJobFromResultSet(ResultSet result, DataSource dataSource, Properties config) throws ClassNotFoundException, SQLException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Class<?> jobClazz = Class.forName(result.getString(2));
		Constructor<?> jobConstructor = jobClazz.getConstructor(DataSource.class, Properties.class, long.class, List.class);
		return (Job) jobConstructor.newInstance(dataSource, config, result.getLong(1), StringUtils.delimitedToList(result.getString(3)));
	}
	
	/**
	 * Update a job record with the necessary details when it's successfully finshed running. The {@code status} is set to
	 * {@link Job.FINISHED_STATE}, and the {@code ended} field is given the current time.
	 * @param conn A database connection
	 * @param jobID The job that has been started
	 * @throws DatabaseException If an error occurs while updating the record
	 * @throws NoSuchJobException If the specified job doesn't exist
	 */
	public static void finishJob(DataSource dataSource, long jobID) throws MissingParamException, DatabaseException, NoSuchJobException {
		
		MissingParam.checkMissing(dataSource, "dataSource");
		
		if (!jobExists(dataSource, jobID)) {
			throw new NoSuchJobException(jobID);
		}

		Connection connection = null;
		PreparedStatement stmt = null;
		
		try {
			connection = dataSource.getConnection();
			stmt = connection.prepareStatement(END_JOB_STATEMENT);
			stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
			stmt.setLong(2, jobID);
			stmt.execute();
		} catch (SQLException e) {
			throw new DatabaseException("An error occurred while setting the job to 'finished' state", e);
		} finally {
			try {
				if (null != stmt) {
					stmt.close();
				}
				if (null != connection) {
					connection.close();
				}
			} catch (SQLException e) {
				// Do nothing.
			}
		}
	}
	
	/**
	 * Update a job record indicating that the job failed due to an error
	 * @param conn A database connection
	 * @param jobID The ID of the job
	 * @param error The error that caused the job to fail
	 * @throws DatabaseException If an error occurs while updating the database
	 * @throws NoSuchJobException If the specified job does not exist
	 */
	public static void errorJob(DataSource dataSource, long jobID, Throwable error) throws MissingParamException, DatabaseException, NoSuchJobException {
		
		MissingParam.checkMissing(dataSource, "dataSource");
		
		if (!jobExists(dataSource, jobID)) {
			throw new NoSuchJobException(jobID);
		}

		Connection connection = null;
		PreparedStatement stmt = null;
		
		try {
			connection = dataSource.getConnection();
			stmt = connection.prepareStatement(ERROR_JOB_STATEMENT);
			stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
			stmt.setString(2, StringUtils.stackTraceToString(error));
			stmt.setLong(3, jobID);
			stmt.execute();
		} catch (SQLException e) {
			throw new DatabaseException("An error occurred while setting the error state of the job", e);
		} finally {
			try {
				if (null != stmt) {
					stmt.close();
				}
				if (null != connection) {
					connection.close();
				}
			} catch (SQLException e) {
				// Do nothing.
			}
		}
	}

	/**
	 * Set the progress for a job. The progress must be a percentage (between 0 and 100 inclusive)
	 * @param conn A database connection
	 * @param jobID The ID of the job
	 * @param progress The progress
	 * @throws BadProgressException If the progress value is invalid
	 * @throws NoSuchJobException If the specified job does not exist
	 * @throws DatabaseException If an error occurs while storing the progress in the database
	 */
	public static void setProgress(DataSource dataSource, long jobID, double progress) throws MissingParamException, BadProgressException, NoSuchJobException, DatabaseException {

		MissingParam.checkMissing(dataSource, "dataSource");
		
		if (progress < 0 || progress > 100) {
			throw new BadProgressException();
		}
		
		if (!jobExists(dataSource, jobID)) {
			throw new NoSuchJobException(jobID);
		}
		
		Connection connection = null;
		PreparedStatement stmt = null;
		
		try {
			connection = dataSource.getConnection();
			stmt = connection.prepareStatement(SET_PROGRESS_STATEMENT);
			stmt.setDouble(1, progress);
			stmt.setLong(2, jobID);
			stmt.execute();
		} catch (SQLException e) {
			throw new DatabaseException("An error occurred while setting the status", e);
		} finally {
			try {
				if (null != stmt) {
					stmt.close();
				}
				if (null != connection) {
					connection.close();
				}
			} catch (SQLException e) {
				// Do nothing.
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
	private static boolean jobExists(DataSource dataSource, long jobID) throws MissingParamException, DatabaseException {

		MissingParam.checkMissing(dataSource, "dataSource");
		
		boolean jobExists = false;
		
		Connection connection = null;
		PreparedStatement stmt = null;
		
		try {
			connection = dataSource.getConnection();
			stmt = connection.prepareStatement(FIND_JOB_QUERY);
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
			try {
				if (null != stmt) {
					stmt.close();
				}
				if (null != connection) {
					connection.close();
				}
			} catch (SQLException e) {
				// Do nothing.
			}
		}
		
		return jobExists;
	}
	
	/**
	 * Retrieve the next queued job (i.e. the job with the oldest submission date)
	 * from the database 
	 * @param dataSource A data source
	 * @return The next queued job, or {@code null} if there are no jobs.
	 * @throws MissingParamException If the data source is not supplied
	 * @throws DatabaseException If an error occurs while retrieving details from the database.
	 */
	public static Job getNextJob(DataSource dataSource, Properties config) throws MissingParamException, DatabaseException {
		
		MissingParam.checkMissing(dataSource, "dataSource");

		Job job = null;
		Connection connection = null;
		PreparedStatement stmt = null;
		
		try {
			connection = dataSource.getConnection();
			stmt = connection.prepareStatement(GET_NEXT_JOB_QUERY);
			
			ResultSet result = stmt.executeQuery();
			if (result.next()) {
				job = getJobFromResultSet(result, dataSource, config);
			}
		} catch (SQLException|ClassNotFoundException|NoSuchMethodException|InstantiationException|IllegalAccessException|IllegalArgumentException|InvocationTargetException e) {
			// We handle all exceptions as DatabaseExceptions.
			// The fact is that invalid jobs should never get into the database in the first place.
			throw new DatabaseException("Error while retrieving details for next queued job", e);
		} finally {
			try {
				if (null != stmt) {
					stmt.close();
				}
				if (null != connection) {
					connection.close();
				}
			} catch (SQLException e) {
				// Do nothing.
			}
		}
		
		return job;
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
				Constructor<?> jobConstructor = jobClazz.getConstructor(DataSource.class, Properties.class, long.class, List.class);
				Type[] constructorGenericTypes = jobConstructor.getGenericParameterTypes();
				if (constructorGenericTypes.length != 4) {
					checkResult = CLASS_CHECK_INVALID_CONSTRUCTOR;
				} else {
					if (!(constructorGenericTypes[3] instanceof ParameterizedType)) {
						checkResult = CLASS_CHECK_INVALID_CONSTRUCTOR;
					} else {
						Type[] actualTypeArguments = ((ParameterizedType) constructorGenericTypes[3]).getActualTypeArguments();
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