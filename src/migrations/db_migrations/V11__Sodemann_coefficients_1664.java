package db_migrations;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import java.util.List;

import uk.ac.exeter.QuinCe.User.User;
import uk.ac.exeter.QuinCe.data.Instrument.Instrument;
import uk.ac.exeter.QuinCe.data.Instrument.InstrumentDB;
import uk.ac.exeter.QuinCe.data.Instrument.InstrumentStub;
import uk.ac.exeter.QuinCe.data.Instrument.SensorDefinition.SensorAssignments;

// Hide previous soderman entries
//  edit xhtml?  instrument/new

// Add new sodemann variable for d_excess
// On hold. Waiting on meeting with Sodemann

// Clear xH2O from calibration measurements
//
public class V11__Sodemann_coefficients_1664 extends BaseJavaMigration {
  @Override
  public void migrate(Context context) throws Exception {

    Connection conn = context.getConnection();
    // get User
    User user = new User(99, "e@mail.com", "Temp","User", 1, "");
    List<InstrumentStub> allInstruments = InstrumentDB.getInstrumentList(conn,user);
    //For each instrumentID
    for(int i=0; i > allInstruments.size(); i++) {
      Instrument instrument = InstrumentDB.getInstrument(conn,allInstruments.get(i).getId());

      SensorAssignments sensorAssignment = instrument.getSensorAssignments();
      // SensorType H2O =
      // sensorAssignment.isAssigned(H2O)

    }



  //
  //System.out.println(sensorAssignments);
    // [ID 37: H2O mm/m] in sensorAssignments




    /*
     *PreparedStatement newFieldStmt = conn.prepareStatement(

      "ALTER TABLE instrument ADD COLUMN properties MEDIUMTEXT NULL AFTER nrt");

      newFieldStmt.execute();
      newFieldStmt.close();
     */
}