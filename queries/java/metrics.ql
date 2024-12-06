import java
import semmle.code.java.metrics.MetricElement

/**
 * Extracts class metrics:
 * - Number of Methods (NOM)
 * - Number of Fields (NOF)
 * - Cyclomatic Complexity (CC)
 * - Halstead Volume (HV)
 * - Maintainability Index (MI)
 */
from Class c
select
    c.getName(),                             // Class name
    count(c.getMethods()),                   // Number of methods (NOM)
    count(c.getFields()),                    // Number of fields (NOF)
    c.getMetrics().getCyclomaticComplexity(), // Cyclomatic Complexity (CC)
    c.getMetrics().getHalsteadVolume(),      // Halstead Volume (HV)
    c.getMetrics().getMaintainabilityIndex() // Maintainability Index (MI)
