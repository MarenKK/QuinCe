package uk.ac.exeter.QuinCe.data.Dataset.DataReduction;

import java.sql.Connection;
import java.util.List;

import uk.ac.exeter.QuinCe.data.Dataset.DateColumnGroupedSensorValues;
import uk.ac.exeter.QuinCe.data.Dataset.Measurement;
import uk.ac.exeter.QuinCe.data.Instrument.Instrument;
import uk.ac.exeter.QuinCe.data.Instrument.Calibration.CalibrationSet;
import uk.ac.exeter.QuinCe.data.Instrument.SensorDefinition.InstrumentVariable;

/**
 * Factory class for Data Reducers
 * @author Steve Jones
 *
 */
public class DataReducerFactory {

  /**
   * Get the Data Reducer for a given variable and initialise it
   * @param variable The variable
   * @return The Data Reducer
   * @throws DataReductionException If the reducer cannot be retreived 
   */
  public static DataReducer getReducer(
      Connection conn, Instrument instrument, InstrumentVariable variable,
      CalibrationSet calibrationSet,
      List<Measurement> allMeasurements,
      DateColumnGroupedSensorValues groupedSensorValues)
      throws DataReductionException {
    
    DataReducer reducer;

    try {
      switch (variable.getName()) {
      case "Underway Marine pCO₂": {
        reducer = new UnderwayMarinePco2Reducer(allMeasurements, groupedSensorValues, calibrationSet);
        break;
      }
      default: {
        throw new DataReductionException("Cannot find reducer for variable " + variable.getName());
      }
      }
    } catch (Exception e) {
      throw new DataReductionException("Cannot initialise data reducer", e);
    }
      
    return reducer;
  }
}
