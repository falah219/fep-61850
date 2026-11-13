use dashmap::DashMap;
use serde_json::Value;
use std::sync::Arc;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct FormulaConfig {
    pub scaling_type: String,
    pub scale: f64,
    pub offset: f64,
    pub span_low: f64,
    pub span_high: f64,
}

impl Default for FormulaConfig {
    fn default() -> Self {
        Self {
            scaling_type: "No Scale".to_string(),
            scale: 1.0,
            offset: 0.0,
            span_low: 0.0,
            span_high: 100.0,
        }
    }
}

pub struct FormulaResolver {
    mapping: Arc<DashMap<String, FormulaConfig>>,
}

impl FormulaResolver {
    pub fn new() -> Self {
        Self {
            mapping: Arc::new(DashMap::new()),
        }
    }

    pub fn load(&self, formulas: Vec<(String, String, f64, f64, f64, f64)>) {
        self.mapping.clear();
        for (tag, scaling_type, scale, offset, span_low, span_high) in formulas {
            let config = FormulaConfig {
                scaling_type,
                scale,
                offset,
                span_low,
                span_high,
            };
            self.mapping.insert(tag, config);
        }
    }

    pub fn get_formula(&self, tag: &str) -> Option<FormulaConfig> {
        self.mapping.get(tag).map(|v| v.clone())
    }

    pub fn add_formula(&self, tag: String, config: FormulaConfig) {
        self.mapping.insert(tag, config);
    }

    pub fn remove_formula(&self, tag: &str) {
        self.mapping.remove(tag);
    }

    pub fn update_formula(&self, tag: &str, config: FormulaConfig) {
        if self.mapping.contains_key(tag) {
            self.mapping.insert(tag.to_string(), config);
        }
    }

    pub fn reload(&self) {
        self.mapping.clear();
    }

    pub fn get_count(&self) -> usize {
        self.mapping.len()
    }

    pub fn apply_formula(&self, tag: &str, value: f64) -> f64 {
        match self.mapping.get(tag) {
            Some(formula) => apply_scaling(
                value,
                &formula.scaling_type,
                formula.scale,
                formula.offset,
                formula.span_low,
                formula.span_high,
            ),
            None => value,
        }
    }

    pub fn apply_formula_to_string(&self, tag: &str, value: &str) -> String {
        if value == "null" || value.is_empty() {
            return value.to_string();
        }

        match value.parse::<f64>() {
            Ok(num_value) => self.apply_formula(tag, num_value).to_string(),
            Err(_) => value.to_string(),
        }
    }

    pub fn apply_formula_to_value(&self, tag: &str, value: Value) -> Value {
        match &value {
            Value::Number(n) => {
                if let Some(num) = n.as_f64() {
                    let scaled = self.apply_formula(tag, num);
                    Value::Number(serde_json::Number::from_f64(scaled).unwrap_or_else(|| n.clone()))
                } else if let Some(num) = n.as_i64() {
                    let scaled = self.apply_formula(tag, num as f64);
                    Value::Number(serde_json::Number::from_f64(scaled).unwrap_or_else(|| n.clone()))
                } else {
                    value
                }
            }
            Value::String(s) => {
                if s == "null" || s.is_empty() {
                    value
                } else if let Ok(num) = s.parse::<f64>() {
                    serde_json::Number::from_f64(self.apply_formula(tag, num))
                        .map_or_else(|| value.clone(), Value::Number)
                } else {
                    value
                }
            }
            _ => value,
        }
    }
}

impl Default for FormulaResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for FormulaResolver {
    fn clone(&self) -> Self {
        Self {
            mapping: Arc::clone(&self.mapping),
        }
    }
}

unsafe impl Send for FormulaResolver {}
unsafe impl Sync for FormulaResolver {}

fn apply_scaling(
    value: f64,
    scaling_type: &str,
    scale: f64,
    offset: f64,
    span_low: f64,
    span_high: f64,
) -> f64 {
    match scaling_type {
        "No Scale" | "" => value,
        "Scale 0-100% Input to Span" => (span_high - span_low) * (value / 100.0) + span_low,
        "Linear Scale, MX+B" => scale * value + offset,
        "Scale Defined Input H/L to Span" => {
            if (scale - offset).abs() < f64::EPSILON {
                span_low
            } else {
                (span_high - span_low) * ((value - offset) / (scale - offset)) + span_low
            }
        }
        "Scale 12-Bit Input to Span" => (span_high - span_low) * (value / 4095.0) + span_low,
        "Scale 0-100% Square Root Input" => {
            if value < 0.0 {
                span_low
            } else {
                (span_high - span_low) * (value / 100.0).sqrt() + span_low
            }
        }
        "Square Root of (Input/(F2-F1)) to Span" => {
            if (offset - scale).abs() < f64::EPSILON || (value - scale) < 0.0 {
                span_low
            } else {
                (span_high - span_low) * ((value - scale) / (offset - scale)).sqrt() + span_low
            }
        }
        _ => {
            warn!("[FORMULA] Unknown scaling type: {}", scaling_type);
            value
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_scale() {
        let result = apply_scaling(50.0, "No Scale", 1.0, 0.0, 0.0, 100.0);
        assert_eq!(result, 50.0);
    }

    #[test]
    fn test_scale_0_100_to_span() {
        let result = apply_scaling(50.0, "Scale 0-100% Input to Span", 1.0, 0.0, 0.0, 100.0);
        assert_eq!(result, 50.0);

        let result = apply_scaling(50.0, "Scale 0-100% Input to Span", 1.0, 0.0, 0.0, 200.0);
        assert_eq!(result, 100.0);
    }

    #[test]
    fn test_linear_scale() {
        let result = apply_scaling(10.0, "Linear Scale, MX+B", 2.0, 5.0, 0.0, 100.0);
        assert_eq!(result, 25.0);
    }

    #[test]
    fn test_scale_12_bit() {
        let result = apply_scaling(4095.0, "Scale 12-Bit Input to Span", 1.0, 0.0, 0.0, 100.0);
        assert!((result - 100.0).abs() < 0.01);

        let result = apply_scaling(0.0, "Scale 12-Bit Input to Span", 1.0, 0.0, 0.0, 100.0);
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_formula_resolver() {
        let resolver = FormulaResolver::new();

        resolver.add_formula(
            "TEST_TAG".to_string(),
            FormulaConfig {
                scaling_type: "Linear Scale, MX+B".to_string(),
                scale: 2.0,
                offset: 10.0,
                span_low: 0.0,
                span_high: 100.0,
            },
        );

        let result = resolver.apply_formula("TEST_TAG", 5.0);
        assert_eq!(result, 20.0);

        let result = resolver.apply_formula("UNKNOWN_TAG", 5.0);
        assert_eq!(result, 5.0);
    }
}
