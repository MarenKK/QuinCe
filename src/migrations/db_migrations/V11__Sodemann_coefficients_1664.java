package db_migrations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import uk.ac.exeter.QuinCe.User.User;
import uk.ac.exeter.QuinCe.data.Instrument.InstrumentDB;
import uk.ac.exeter.QuinCe.data.Instrument.InstrumentStub;

// Hide previous soderman entries
//  edit xhtml?  instrument/new

// Add new sodemann variable for d_excess
// On hold. Waiting on meeting with Sodemann

// Clear xH2O from calibration measurements
public class V11__Sodemann_coefficients_1664 extends BaseJavaMigration {
  @Override
  public void migrate(Context context) throws Exception {

    Connection conn = context.getConnection();
    User user = new User(99, "e@mail.com", "Temp", "User", 1, "");
    List<InstrumentStub> allInstruments = InstrumentDB.getInstrumentList(conn,
      user);

    // For each instrumentID
    for (int i = 0; i < allInstruments.size(); i++) {

      // FETCH FILE_DEFINITION_ID WITH INSTRUMENT_ID
      try (PreparedStatement fileDefStmt = conn.prepareStatement(
        "SELECT id FROM file_definition WHERE instrument_id = ? ")) {

        fileDefStmt.setLong(1, allInstruments.get(i).getId());
        ResultSet resultFileDef = fileDefStmt.executeQuery();

        while (resultFileDef.next()) {
          // FETCH ALL SENSOR_TYPES associated with
          // FILE_DEFINITION_ID/INSTURMENT
          Boolean assignedCO2 = false;
          Boolean assignedH2O = false;

          try (PreparedStatement sensorTypeStmt = conn.prepareStatement(
            "SELECT sensor_type FROM file_column WHERE file_definition_id = ?")) {

            sensorTypeStmt.setLong(1, resultFileDef.getInt(1));
            ResultSet resultSensorType = sensorTypeStmt.executeQuery();

            while (resultSensorType.next()) {// && !assignedH2O) {
              System.out.println(resultSensorType.getString(1));
              if (resultSensorType.getString(1).equals("8")) {
                assignedH2O = true;
              } else if (resultSensorType.getString(1).equals("9")) {
                assignedCO2 = true;
              }
            }

            if (assignedCO2 && !assignedH2O) {
              try (PreparedStatement coeffStmt = conn.prepareStatement(

                "SELECT id, coefficients FROM calibration WHERE instrument_id = ? AND type = ? ")) {

                coeffStmt.setLong(1, allInstruments.get(i).getId());
                coeffStmt.setString(2, "EXTERNAL_STANDARD");

                ResultSet resultCoeff = coeffStmt.executeQuery();
                while (resultCoeff.next()) {
                  int databaseId = resultCoeff.getInt(1);
                  String[] coeffs = resultCoeff.getString(2).split(";");
                  String xCO2 = coeffs[0];
                  System.out.println(coeffs.toString());

                  try (PreparedStatement setCoeffStmt = conn.prepareStatement(

                    "UPDATE calibration SET coefficients = ? WHERE id = ?")) {

                    setCoeffStmt.setString(1, xCO2);
                    setCoeffStmt.setInt(2, databaseId);

                    setCoeffStmt.executeUpdate();

                  }
                }
              }
            }
          }

        }
      }
    }
  }
}

//
// System.out.println(sensorAssignments);
// xH₂O (with standards)

/*
 * PreparedStatement newFieldStmt = conn.prepareStatement(
 *
 * "ALTER TABLE instrument ADD COLUMN properties MEDIUMTEXT NULL AFTER nrt");
 *
 * newFieldStmt.execute(); newFieldStmt.close();
 */
