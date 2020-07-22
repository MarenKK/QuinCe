package db_migrations;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import uk.ac.exeter.QuinCe.data.Files.DataFile;

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

    // Fetch local path of FileStore
    String FileStorePath = getFileStorePath();

    MessageDigest shaDigest = MessageDigest.getInstance("SHA-256");

    int fileId = 0;
    int instrumentId = 0;

    try (PreparedStatement getStmt = conn
      .prepareStatement("SELECT id, file_definition_id  FROM data_file")) {

      try (ResultSet fileIds = getStmt.executeQuery()) {
        while (fileIds.next()) {
          fileId = fileIds.getInt(1);
          instrumentId = fileIds.getInt(2);
          String filepath = FileStorePath + "/" + instrumentId + "/" + fileId;

          if (new File(filepath).exists()) {
            // Calculate checksum using SHA-256 algorithm
            String shaChecksum = getFileChecksum(shaDigest, filepath);
            updateTable(conn, fileId, shaChecksum);
          }
        }
      }
    }
  }

  private void updateTable(Connection conn, int fileId, String shaChecksum)
    throws SQLException {
    // Add column hashsum to data_file table
    try (PreparedStatement newMeasIdStmt = conn
      .prepareStatement("UPDATE " + "data_file SET hashsum = ? WHERE id = ?")) {

      newMeasIdStmt.setString(1, shaChecksum);
      newMeasIdStmt.setLong(2, fileId);

      newMeasIdStmt.execute();
    }
  }

  private String getFileStorePath() throws FileNotFoundException, IOException {
    //
    String QuinceProperties = "configuration/quince.properties";

    Properties result = new Properties();
    result.load(new FileInputStream(new File(QuinceProperties)));

    return result.getProperty("filestore");
  }

  private void addHashsumColumn(Connection conn) throws SQLException {
    // Add column hashsum to data_file table
    try (PreparedStatement addStmt = conn.prepareStatement(
      "ALTER TABLE data_file ADD hashsum varchar(64) DEFAULT NULL")) {
      addStmt.execute();
    }
  }

  private String getFileChecksum(MessageDigest md, String filepath)
    throws IOException {

    return DataFile.calculateHashsum(Files.readAllBytes(Paths.get(filepath)));

  }
}
