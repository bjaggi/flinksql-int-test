package io.confluent.flink.examples.helper;

import org.apache.flink.types.Row;
import java.util.Objects;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowComparator {
    private static final Logger logger = LoggerFactory.getLogger(RowComparator.class);
    
    /**
     * Compares two Flink Row objects for equality
     * @param expected The expected Row
     * @param actual The actual Row
     * @return true if rows are equal, false otherwise
     */
    public static boolean areRowsEqual(Row expected, Row actual) {
        if (expected == actual) return true;
        if (expected == null || actual == null) return false;
        if (expected.getArity() != actual.getArity()) return false;

        for (int i = 0; i < expected.getArity(); i++) {
            Object expectedField = expected.getField(i);
            Object actualField = actual.getField(i);

            if (!Objects.equals(expectedField, actualField)) {
                logger.info("Field mismatch at position {}:", i);
                logger.info("Expected: {}", expectedField);
                logger.info("Actual: {}", actualField);
                return false;
            }
        }
        return true;
    }

    /**
     * Compares two Flink Row objects and returns a detailed comparison result
     * @param expected The expected Row
     * @param actual The actual Row
     * @return ComparisonResult containing detailed comparison information
     */
    public static ComparisonResult compareRows(Row expected, Row actual) {
        ComparisonResult result = new ComparisonResult();
        
        if (expected == actual) {
            result.setEqual(true);
            return result;
        }
        
        if (expected == null || actual == null) {
            result.setEqual(false);
            result.setMessage("One of the rows is null");
            return result;
        }
        
        if (expected.getArity() != actual.getArity()) {
            result.setEqual(false);
            result.setMessage("Row arity mismatch: expected=" + expected.getArity() + 
                            ", actual=" + actual.getArity());
            return result;
        }

        Set<String> fieldNames = expected.getFieldNames(true);
        List<String> fieldNamesList = fieldNames != null ? new ArrayList<>(fieldNames) : null;

        // Compare all columns except the last one (headers)
        for (int i = 0; i < expected.getArity() - 1; i++) {
            Object expectedField = expected.getField(i);
            Object actualField = actual.getField(i);

            String fieldName = fieldNamesList != null && i < fieldNamesList.size() 
                ? fieldNamesList.get(i) 
                : "field_" + i;

            logger.info("Comparing field '{}' (position {}):", fieldName, i);
            logger.info("Expected: {} ({})", expectedField, 
                expectedField != null ? expectedField.getClass().getName() : "null");
            logger.info("Actual: {} ({})", actualField, 
                actualField != null ? actualField.getClass().getName() : "null");

            // Simple null check
            if (expectedField == null && actualField == null) {
                logger.info("Both values are null, considering equal");
                continue;
            }

            // Compare values
            boolean isEqual = false;
            if (expectedField != null && actualField != null) {
                if (expectedField instanceof Number && actualField instanceof Number) {
                    isEqual = ((Number) expectedField).doubleValue() == ((Number) actualField).doubleValue();
                } else {
                    isEqual = expectedField.toString().equals(actualField.toString());
                }
            }

            if (!isEqual) {
                result.setEqual(false);
                result.setMessage(String.format(
                    "Field '%s' (position %d) mismatch:\nExpected: %s\nActual:   %s",
                    fieldName, i, expectedField, actualField
                ));
                return result;
            }
        }

        result.setEqual(true);
        return result;
    }

    /**
     * Class to hold detailed comparison results
     */
    public static class ComparisonResult {
        private boolean equal;
        private String message;

        public boolean isEqual() {
            return equal;
        }

        public void setEqual(boolean equal) {
            this.equal = equal;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public String toString() {
            return equal ? "Rows are equal" : "Rows are not equal: " + message;
        }
    }
} 