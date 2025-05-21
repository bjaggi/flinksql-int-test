package io.confluent.flink.examples.helper;

import org.apache.flink.types.Row;
import java.util.Objects;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowComparator {
    private static final Logger logger = LoggerFactory.getLogger(RowComparator.class);
    
    /**
     * Compares two Flink Row objects for equality by converting all elements to strings and comparing as sets
     * @param expected The expected Row
     * @param actual The actual Row
     * @return true if rows contain the same elements regardless of order, false otherwise
     */
    public static boolean areRowsEqual(Row expected, Row actual) {
        if (expected == actual) return true;
        if (expected == null || actual == null) return false;
        if (expected.getArity() != actual.getArity()) return false;

        Set<String> expectedSet = new HashSet<>();
        Set<String> actualSet = new HashSet<>();

        // Convert all fields to strings and add to sets
        for (int i = 0; i < expected.getArity(); i++) {
            expectedSet.add(String.valueOf(expected.getField(i)));
            actualSet.add(String.valueOf(actual.getField(i)));
        }

        // Compare sets
        boolean isEqual = expectedSet.equals(actualSet);
        if (!isEqual) {
            logger.info("Row mismatch:");
            logger.info("Expected set: {}", expectedSet);
            logger.info("Actual set: {}", actualSet);
        }
        return isEqual;
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
        
        if (expected.getArity() != actual.getArity()) {
            result.setEqual(false);
            result.setMessage("Row arity mismatch: expected=" + expected.getArity() + 
                            ", actual=" + actual.getArity());
            return result;
        }

        Set<String> expectedSet = new HashSet<>();
        Set<String> actualSet = new HashSet<>();

        // Convert all fields to strings and add to sets
        for (int i = 0; i < expected.getArity(); i++) {
            expectedSet.add(String.valueOf(expected.getField(i)));
            actualSet.add(String.valueOf(actual.getField(i)));
        }

        // Compare sets
        if (!expectedSet.equals(actualSet)) {
            result.setEqual(false);
            result.setMessage(String.format(
                "Row mismatch:\nExpected set: %s\nActual set:   %s",
                expectedSet, actualSet
            ));
            return result;
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