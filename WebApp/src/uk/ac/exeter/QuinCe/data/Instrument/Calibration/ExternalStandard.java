package uk.ac.exeter.QuinCe.data.Instrument.Calibration;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import uk.ac.exeter.QuinCe.data.Instrument.Instrument;
import uk.ac.exeter.QuinCe.data.Instrument.InstrumentDB;
import uk.ac.exeter.QuinCe.data.Instrument.RunTypes.RunTypeCategoryConfiguration;
import uk.ac.exeter.QuinCe.data.Instrument.SensorDefinition.SensorsConfiguration;
import uk.ac.exeter.QuinCe.utils.ParameterException;
import uk.ac.exeter.QuinCe.web.system.ResourceManager;

/**
 * Represents a external standard calibration
 *
 * @author Steve Jones
 *
 */
public class ExternalStandard extends Calibration {

  /**
   * Create an empty external standard placeholder that isn't bound to a
   * particular standard
   *
   * @param instrumentId
   *          The instrument ID
   */
  public ExternalStandard(long instrumentId) {
    super(instrumentId, ExternalStandardDB.EXTERNAL_STANDARD_CALIBRATION_TYPE);
    
  }

  /**
   * Creates an empty external standard for a specified standard
   *
   * @param instrumentid
   *          The instrument ID
   * @param standard
   *          The standard
   */
  protected ExternalStandard(long instrumentid, String standard) {
    super(instrumentid, ExternalStandardDB.EXTERNAL_STANDARD_CALIBRATION_TYPE,
      standard);
  }

  /**
   * Construct a complete external standard object with all data
   *
   * @param instrumentId
   *          The instrument ID
   * @param target
   *          The target external standard
   * @param deploymentDate
   *          The deployment date
   * @param coefficients
   *          The standard concentration
   * @throws ParameterException
   *           If the calibration details are invalid
   */
  protected ExternalStandard(long id, long instrumentId, String target,
    LocalDateTime deploymentDate, List<Double> coefficients)
    throws ParameterException {
    super(id, instrumentId,
      ExternalStandardDB.EXTERNAL_STANDARD_CALIBRATION_TYPE, target);

    if (null != target) {
      setDeploymentDate(deploymentDate);
      setCoefficients(coefficients);
      if (!validate()) {
        throw new ParameterException("Deployment date/coefficients",
          "Calibration deployment is invalid");
      }
    }
  }
@Override
  public List<String> getCoefficientNames() {
	  try {
		  DataSource dataSrc = ResourceManager.getInstance().getDBDataSource();
		  SensorsConfiguration sensorConfig = ResourceManager.getInstance().getSensorsConfiguration();
		  RunTypeCategoryConfiguration runTypeConfig = ResourceManager.getInstance().getRunTypeCategoryConfiguration();
		  
		  Instrument instrument = InstrumentDB.getInstrument(dataSrc, instrumentId, sensorConfig, runTypeConfig);
		  List<String> test = new ArrayList<String>(instrument.getInternalCalibrations());
		  System.out.println(test);
		  return test;
		  }
	  catch(Exception e) {
		  return null;
	  }
  }

  @Override
  public String buildHumanReadableCoefficients() {
    String result = "Not set";

    if (null != coefficients) {
      result = String.valueOf(coefficients.get(0).getValue());
    }

    return result;
  }

  /**
   * Get the concentration of the external standard
   *
   * @return The concentration
   */
  public double getConcentration() {
    if (null == coefficients) {
      initialiseCoefficients();
    }

    return coefficients.get(0).getValue();
  }

  /**
   * Set the concentration of the external standard
   *
   * @param concentration
   *          The concentration
   */
  public void setConcentration(double concentration) {
    if (null == coefficients) {
      initialiseCoefficients();
    }

    coefficients.set(0,
      new CalibrationCoefficient(getCoefficientNames().get(0), concentration));
  }

  @Override
  public boolean coefficientsValid() {
    boolean result = true;

    if (null != coefficients) {
      if (coefficients.size() != 2) {
        result = false;
      } else {
        if (getConcentration() < 0) {
          result = false;
        }
        if (getCoefficients().get(1).getValue() != 0.0) {
          // xH2O must be zero
          result = false;
        }
      }
    }

    return result;
  }

  @Override
  public Double calibrateValue(Double rawValue) {
    return rawValue;
  }

  @Override
  public List<CalibrationCoefficient> getEditableCoefficients() {
    // Only the CO2 concentration is editable, and it's the first coefficient.
    return getCoefficients().subList(0, 1);
  }
}
