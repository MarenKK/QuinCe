package db_migrations;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import uk.ac.exeter.QuinCe.data.Files.DataFile;
import uk.ac.exeter.QuinCe.data.Files.DataFileDB;

/**
 * Migration to add the hashsum of each file entry in the database.
 *
 * @author Maren Karlsen
 *
 */

public class V9__file_ingester_hashsums_1777 extends BaseJavaMigration {

  @Override
  public void migrate(Context context) throws Exception {

    Connection conn = context.getConnection();

    // Update data_file table to include hashsum column
    addHashsumColumn(conn);

    // calculate hashsums for all entries in data_file
    String filePath = "configuration/qc_routines_config.csv";

    System.out.println(filePath);

    // FETCH LIST OF FILES
    Properties result = new Properties();
    result.load(new FileInputStream(new File(filePath)));
    System.out.println(result);

    List<DataFile> Files = DataFileDB.getFiles(conn, result, null);

    for (DataFile File : Files) {
      System.out.println(File);

    }

    /*
     * Get list of files using dataFileDB.getFiles
     *
     * instrument = Null returns all files appConfig contains the path to
     * FileStore
     *
     * Use relational path from QuinCe-folder to access
     * configuration/quince.properties Use lines from
     * ResourceManager.loadConfiguration to create a properties-object See
     * CreateNrtDataset line 98 for inspiration
     *
     *
     *
     * For each item in list: - get content - create hashsum - update table
     * using SQL query
     *
     */

  }

  private List<Long> getFiles(Connection conn) throws SQLException {

    // Get the record count

    int fileId;
    int instrumentId;

    try (PreparedStatement getStmt = conn
      .prepareStatement("SELECT id, file_definition_id  FROM data_file")) {

      try (ResultSet fileIds = getStmt.executeQuery()) {
        while (fileIds.next()) {
          fileId = fileIds.getInt(1);
          instrumentId = fileIds.getInt(2);
          System.out.println(fileId);
        }
      }
    }

    return null;
  }

  private void addHashsumColumn(Connection conn) throws SQLException {
    // Add column hashsum to data_file table
    try (PreparedStatement addStmt = conn.prepareStatement(
      "ALTER TABLE data_file ADD hashsum varchar(64) DEFAULT NULL")) {
      addStmt.execute();
    }
  }

}
