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
      SensorsConfiguration sensorConfig = ResourceManager.getInstance()
        .getSensorsConfiguration();
      RunTypeCategoryConfiguration runTypeConfig = ResourceManager.getInstance()
        .getRunTypeCategoryConfiguration();

      Instrument instrument = InstrumentDB.getInstrument(dataSrc, instrumentId,
        sensorConfig, runTypeConfig);
      return new ArrayList<String>(instrument.getInternalCalibrations());
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public Double calibrateValue(Double rawValue) {
    return rawValue;
  }
}
