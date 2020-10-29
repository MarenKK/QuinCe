package uk.ac.exeter.QuinCe.data.Instrument.SensorDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import uk.ac.exeter.QuinCe.data.Dataset.QC.Flag;
import uk.ac.exeter.QuinCe.data.Dataset.QC.InvalidFlagException;
import uk.ac.exeter.QuinCe.web.system.ResourceManager;

/**
 * Class to hold the sensors required for a given variable.
 *
 * The sensors are split into:
 *
 * CORE: The sensor that measures the actual variable REQUIRED: The additional
 * sensors needed to perform data reduction
 *
 * Any sensors that may be required because CORE or REQUIRED depend on them are
 * not included; these are calculated on the fly elsewhere.
 *
 * @author Steve Jones
 *
 */
public class Variable {

  /**
   * The variable's database ID
   */
  private long id;

  /**
   * The variable name
   */
  private String name;

  /**
   * IDs and Labels for this variable's attributes
   */
  LinkedHashMap<String, String> attributes;

  /**
   * The core SensorType
   */
  private SensorType coreSensorType;

  /**
   * The other sensors required for data reduction
   */
  private List<SensorType> requiredSensorTypes;

  /**
   * The cascades from Questionable flags for each required SensorType to the
   * Core SensorType flag
   */
  private Map<SensorType, Flag> questionableCascades;

  /**
   * The cascades from Bad flags for each required SensorType to the Core
   * SensorType flag
   */
  private Map<SensorType, Flag> badCascades;

  /**
   * Main constructor using SensorType ids
   *
   * @param id
   *          The variable's database ID
   * @param name
   *          The variable name
   * @param coreSensorTypeId
   *          The core SensorType's ID
   * @param requiredSensorTypeIds
   *          The other required SensorTypes' IDs
   * @throws SensorTypeNotFoundException
   *           If any SensorTypes are not found
   * @throws SensorConfigurationException
   *           If the parameters are not internally consistent
   * @throws InvalidFlagException
   *           If any cascade flags are invalid
   */
  protected Variable(SensorsConfiguration sensorConfig, long id, String name,
    LinkedHashMap<String, String> attributes, long coreSensorTypeId,
    List<Long> requiredSensorTypeIds, List<Integer> questionableCascades,
    List<Integer> badCascades) throws SensorTypeNotFoundException,
    SensorConfigurationException, InvalidFlagException {

    this.id = id;
    this.name = name;
    this.attributes = attributes;

    if (coreSensorTypeId == -1) {
      coreSensorType = null;
    } else {
      coreSensorType = sensorConfig.getSensorType(coreSensorTypeId);
      if (coreSensorType.hasParent()) {
        throw new SensorConfigurationException(
          "Core sensor type cannot be a child (ID " + coreSensorTypeId + ")");
      }
    }

    if (questionableCascades.size() != requiredSensorTypeIds.size()) {
      throw new SensorConfigurationException(
        "Questionable cascades do not match required sensors");
    }

    if (badCascades.size() != requiredSensorTypeIds.size()) {
      throw new SensorConfigurationException(
        "Bad cascades do not match required sensors");
    }

    this.requiredSensorTypes = new ArrayList<SensorType>(
      requiredSensorTypeIds.size());
    this.questionableCascades = new HashMap<SensorType, Flag>();
    this.badCascades = new HashMap<SensorType, Flag>();

    for (int i = 0; i < requiredSensorTypeIds.size(); i++) {
      SensorType sensorType = sensorConfig
        .getSensorType(requiredSensorTypeIds.get(i));
      if (sensorType.hasParent()) {
        throw new SensorConfigurationException(
          "Required sensor type cannot be a child (ID " + coreSensorTypeId
            + ")");
      }
      requiredSensorTypes.add(sensorType);
      this.questionableCascades.put(sensorType,
        new Flag(questionableCascades.get(i)));
      this.badCascades.put(sensorType, new Flag(badCascades.get(i)));
    }
  }

  /**
   * Get the database ID of this variable
   *
   * @return
   */
  public long getId() {
    return id;
  }

  /**
   * Get the variable name
   *
   * @return The variable name
   */
  public String getName() {
    return name;
  }

  /**
   * Get the core SensorType
   *
   * @return The core SensorType
   */
  public SensorType getCoreSensorType() {
    return coreSensorType;
  }

  /**
   * Get all SensorTypes required for this variable, including both Core and
   * Required types.
   *
   * @return All required SensorTypes
   */
  public List<SensorType> getAllSensorTypes(boolean includePosition) {
    List<SensorType> result = new ArrayList<SensorType>(requiredSensorTypes);

    if (null != coreSensorType) {
      result.add(coreSensorType);
    }

    if (includePosition) {
      result.add(SensorType.LONGITUDE_SENSOR_TYPE);
      result.add(SensorType.LATITUDE_SENSOR_TYPE);
    }
    return result;
  }

  /**
   * Get the cascading flag value for a given SensorType. This indicates what
   * flag should be set on the final calculated value based on the flag of a
   * required SensorType.
   *
   * If the SensorType is not related to the final value in any way, return
   * {@code null}.
   *
   * @param sensorType
   *          The SensorType
   * @param flag
   *          The flag assigned to that SensorType
   * @param sensorAssignments
   *          The current set of sensor assignments for the instrument
   * @return The flag to apply to the final calculated value
   * @throws SensorConfigurationException
   *           If the internal configuration is invalid
   */
  public Flag getCascade(SensorType sensorType, Flag flag,
    SensorAssignments sensorAssignments) throws SensorConfigurationException {

    SensorsConfiguration sensorConfig = ResourceManager.getInstance()
      .getSensorsConfiguration();

    Flag result = null;

    if (sensorType.equals(coreSensorType)
      || sensorType.equals(SensorType.LONGITUDE_SENSOR_TYPE)
      || sensorType.equals(SensorType.LATITUDE_SENSOR_TYPE)) {
      result = flag;
    } else {
      switch (flag.getFlagValue()) {
      case Flag.VALUE_GOOD:
      case Flag.VALUE_ASSUMED_GOOD: {
        result = Flag.ASSUMED_GOOD;
        break;
      }
      case Flag.VALUE_QUESTIONABLE: {
        if (questionableCascades.containsKey(sensorType)) {
          result = questionableCascades.get(sensorType);
        } else {
          SensorType parent = sensorConfig.getParent(sensorType);
          if (null != parent) {
            result = questionableCascades.get(parent);
          }
        }
        break;
      }
      case Flag.VALUE_BAD: {
        if (badCascades.containsKey(sensorType)) {
          result = badCascades.get(sensorType);
        } else {
          SensorType parent = sensorConfig.getParent(sensorType);
          if (null != parent) {
            result = badCascades.get(parent);
          }
        }
        break;
      }
      default: {
        result = flag;
        break;
      }
      }

      /*
       * If result is null here, this means that the supplied SensorType is not
       * in the list of required sensors. Therefore it must be a sensor that one
       * of the core/required sensors depends on. Find that SensorType and get
       * its cascade.
       *
       * If there's more than one matched SensorType, return the worst flag we
       * find
       */
      if (null == result) {
        Set<SensorType> dependingTypes = sensorAssignments
          .getDependents(sensorType);
        for (SensorType dependingType : dependingTypes) {
          Flag dependingCascadeFlag = getCascade(dependingType, flag,
            sensorAssignments);
          if (null == result
            || dependingCascadeFlag.moreSignificantThan(result)) {
            result = dependingCascadeFlag;
          }
        }
      }

      /*
       * If the result is STILL null, then this sensor has no bearing on the
       * final calculated value. We return this as a null.
       */

    }

    return result;
  }

  /**
   * Get a list of variable IDs from a list of variables
   *
   * @param variables
   *          The variables
   * @return The variable IDs
   */
  public static List<Long> getIDsList(List<Variable> variables) {
    List<Long> ids = new ArrayList<Long>(variables.size());
    for (Variable variable : variables) {
      ids.add(variable.getId());
    }
    return ids;
  }

  public boolean hasAttributes() {
    return attributes.size() > 0;
  }

  public LinkedHashMap<String, String> getAttributes() {
    return attributes;
  }

  public static boolean sensorTypeRequired(List<Variable> variables,
    SensorType sensorType) {
    boolean result = false;

    for (Variable variable : variables) {
      if (variable.requiredSensorTypes.contains(sensorType)) {
        result = true;
        break;
      }
    }

    return result;
  }

  public boolean hasInternalCalibrations() {
    boolean result = false;

    for (SensorType sensorType : getAllSensorTypes(false)) {
      if (sensorType.hasInternalCalibration()) {
        result = true;
        break;
      }
    }

    return result;
  }

  public TreeSet<String> getInternalCalibrations() {
    TreeSet<String> result = new TreeSet<String>();

    for (SensorType sensorType : getAllSensorTypes(false)) {
      if (sensorType.hasInternalCalibration()) {
        result.add(sensorType.getName());
      }
    }
    return result;
  }

  // make get Internal Calibrations return list of sensorTypes with internal
  // Calibration

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof Variable))
      return false;
    Variable other = (Variable) obj;
    return id == other.id;
  }

  @Override
  public String toString() {
    return name;
  }
}
