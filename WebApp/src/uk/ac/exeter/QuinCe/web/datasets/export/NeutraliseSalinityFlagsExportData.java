package uk.ac.exeter.QuinCe.web.datasets.export;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.sql.DataSource;

import uk.ac.exeter.QuinCe.data.Dataset.DataSet;
import uk.ac.exeter.QuinCe.data.Dataset.QC.Flag;
import uk.ac.exeter.QuinCe.data.Export.ExportOption;
import uk.ac.exeter.QuinCe.data.Instrument.Instrument;
import uk.ac.exeter.QuinCe.utils.StringUtils;
import uk.ac.exeter.QuinCe.web.datasets.data.Field;
import uk.ac.exeter.QuinCe.web.datasets.data.FieldValue;
import uk.ac.exeter.QuinCe.web.datasets.data.MeasurementDataException;

/**
 * A special version of the {@link ExportData} class that removes the influence
 * of salinity QC flags from the final fCO2 data.
 *
 * <p>
 * For Nuka Arctica, the salinity data is delayed for up to a year before it's
 * available for processing in QuinCe. (At the time of writing, we are even
 * unsure how we would ingest the delayed salinity data.) To get round this, all
 * Nuka Arctica data is pre-processed before being loaded into QuinCe, and has a
 * fake salinity column added with the value taken from the World Ocean Atlas
 * climatology. This causes some salinity values to be flagged as
 * {@link Flag#BAD}, which in turn cascades to the final fCO₂ value, marking
 * them all {@link Flag#QUESTIONABLE}. In this special case, the PI has
 * established that the influence of the fixed salinity value is not sufficient
 * to warrant the flag placed on fCO₂, and therefore it should be removed.
 * </p>
 *
 * <p>
 * This version of the {@link ExportData} class contains a post-processor that
 * removes the influence of the salinity flag from the final fCO₂ values, while
 * ensuring that all other flags are still honoured. It also fixes the Salinity
 * flag as Questionable for all records, since the climatological value is
 * probably reasonable but shouldn't be relied on.
 * </p>
 *
 * @author Steve Jones
 *
 */
@SuppressWarnings("serial")
public class NeutraliseSalinityFlagsExportData extends ExportData {

  private static final String[] CASCADE_FIELDS = { "Intake Temperature",
    "Equilibrator Temperature", "Equilibrator Pressure (absolute)",
    "Equilibrator Pressure (relative)", "Ambient Pressure", "CO₂ in gas" };

  public NeutraliseSalinityFlagsExportData(DataSource dataSource,
    Instrument instrument, DataSet dataSet, ExportOption exportOption)
    throws Exception {
    super(dataSource, instrument, dataSet, exportOption);
  }

  @Override
  public void postProcess() throws MeasurementDataException {

    Field fCO2Field = fieldSets.getField("fCO₂ In Water");
    Field salinityField = fieldSets.getField("Salinity");

    for (LinkedHashMap<Field, FieldValue> dateEntry : values()) {
      FieldValue fCO2Value = dateEntry.get(fCO2Field);
      if (null != fCO2Value) {

        // See if the fCO₂ flag is Questionable. If so, it's possible that it
        // was caused by bad salinity
        if (fCO2Value.getQcFlag().equals(Flag.QUESTIONABLE)) {

          // See if the salinity is marked Bad. If it is, then we must
          // recalculate the fCO₂ flag
          FieldValue salinity = dateEntry.get(salinityField);
          if (null != salinity && salinity.getQcFlag().equals(Flag.BAD)) {

            // We're not using the built-in cascading here, because it's too
            // hard to extract the logic and even harder to extract the logic
            // except salinity. So we're just going to do it manually.

            // The new QC info
            Flag newFCO2Flag = Flag.GOOD;
            List<String> qcComments = new ArrayList<String>();

            // Loop through all the incoming sensor fields
            for (String field : CASCADE_FIELDS) {

              FieldValue value = dateEntry.get(fieldSets.getField(field));
              if (null != value) {
                // If the flag is Good or Questionable, record the QC comment
                // and
                // upgrade the flag if needed
                if (value.getQcFlag() == Flag.BAD
                  || value.getQcFlag().equals(Flag.QUESTIONABLE)) {

                  qcComments.add(value.getQcComment());
                }

                if (value.getQcFlag().moreSignificantThan(newFCO2Flag)) {
                  newFCO2Flag = value.getQcFlag();
                }
              }
            }

            // Set the new QC flag and comments
            fCO2Value.setQC(newFCO2Flag,
              StringUtils.collectionToDelimited(qcComments));

            // Also set the flags on the pCO₂ values
            Field pCO2EqField = fieldSets
              .getField("pCO₂ In Water - Equilibrator Temperature");
            FieldValue pCO2EqValue = dateEntry.get(pCO2EqField);
            pCO2EqValue.setQC(newFCO2Flag,
              StringUtils.collectionToDelimited(qcComments));

            Field pCO2Field = fieldSets.getField("pCO₂ In Water");
            FieldValue pCO2Value = dateEntry.get(pCO2Field);
            pCO2Value.setQC(newFCO2Flag,
              StringUtils.collectionToDelimited(qcComments));
          }
        }
      }

      // Now set the salinity flag
      FieldValue salinityValue = dateEntry.get(salinityField);
      if (null != salinityValue) {
        salinityValue.setQC(Flag.QUESTIONABLE,
          "Climatological value from World Ocean Atlas");
      }
    }
  }
}
