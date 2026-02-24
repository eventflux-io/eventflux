// SPDX-License-Identifier: MIT OR Apache-2.0

// Corresponds to io.eventflux.query.api.definition.StreamDefinition
use crate::query_api::annotation::Annotation;
use crate::query_api::definition::abstract_definition::AbstractDefinition;
use crate::query_api::definition::attribute::{Attribute, Type as AttributeType}; // Assuming Annotation is defined

/// Defines a stream with a unique ID and a list of attributes.
///
/// # Configuration Storage
///
/// StreamDefinition stores configuration properties extracted from SQL WITH clauses.
/// This enables end-to-end flow: SQL parsing → Runtime creation → Factory initialization.
///
/// # Example
///
/// ```sql
/// CREATE STREAM TimerInput (tick LONG) WITH (
///     'type' = 'source',
///     'extension' = 'timer',
///     'timer.interval' = '5000',
///     'format' = 'json'
/// );
/// ```
///
/// The WITH properties are stored in `with_config` and later merged with TOML configuration
/// before being passed to factory.create_initialized().
#[derive(Clone, Debug, PartialEq, Default)] // Added Default
pub struct StreamDefinition {
    // Composition for inheritance from AbstractDefinition
    pub abstract_definition: AbstractDefinition,

    /// Configuration properties from SQL WITH clause
    ///
    /// Stores the FlatConfig extracted during SQL parsing. These properties have
    /// the highest priority in the 4-layer configuration merge:
    ///
    /// Priority (highest to lowest):
    /// 1. SQL WITH (this field)
    /// 2. TOML stream-specific
    /// 3. TOML application-wide
    /// 4. Rust defaults
    ///
    /// None if no WITH clause was specified in SQL.
    pub with_config: Option<crate::core::config::stream_config::FlatConfig>,
}

impl StreamDefinition {
    // Constructor that takes an id, as per Java's `StreamDefinition.id(streamId)`
    pub fn new(id: String) -> Self {
        StreamDefinition {
            abstract_definition: AbstractDefinition::new(id),
            with_config: None,
        }
    }

    // Static factory method `id` from Java
    pub fn id(stream_id: String) -> Self {
        Self::new(stream_id)
    }

    // Builder-style methods, specific to StreamDefinition
    pub fn attribute(mut self, attribute_name: String, attribute_type: AttributeType) -> Self {
        // Check for duplicate attribute names and warn
        if self
            .abstract_definition
            .attribute_list
            .iter()
            .any(|attr| attr.get_name() == &attribute_name)
        {
            eprintln!(
                "Warning: Duplicate attribute '{}' in stream definition",
                attribute_name
            );
        }

        self.abstract_definition
            .attribute_list
            .push(Attribute::new(attribute_name, attribute_type));
        self
    }

    pub fn annotation(mut self, annotation: Annotation) -> Self {
        self.abstract_definition.annotations.push(annotation);
        self
    }

    /// Set configuration properties from SQL WITH clause
    ///
    /// This is typically called during SQL parsing to store the extracted
    /// configuration for later use during runtime creation.
    ///
    /// # Example
    ///
    /// ```
    /// use eventflux::query_api::definition::StreamDefinition;
    /// use eventflux::core::config::stream_config::{FlatConfig, PropertySource};
    ///
    /// let mut config = FlatConfig::new();
    /// config.set("extension", "timer", PropertySource::SqlWith);
    /// config.set("timer.interval", "5000", PropertySource::SqlWith);
    ///
    /// let stream_def = StreamDefinition::new("TimerInput".to_string())
    ///     .with_config(config);
    ///
    /// assert!(stream_def.with_config.is_some());
    /// ```
    pub fn with_config(mut self, config: crate::core::config::stream_config::FlatConfig) -> Self {
        self.with_config = Some(config);
        self
    }

    /// Get reference to WITH configuration if present
    pub fn get_with_config(&self) -> Option<&crate::core::config::stream_config::FlatConfig> {
        self.with_config.as_ref()
    }

    /// Get mutable reference to WITH configuration if present
    pub fn get_with_config_mut(
        &mut self,
    ) -> Option<&mut crate::core::config::stream_config::FlatConfig> {
        self.with_config.as_mut()
    }

    /// Take ownership of WITH configuration, leaving None
    pub fn take_with_config(&mut self) -> Option<crate::core::config::stream_config::FlatConfig> {
        self.with_config.take()
    }

    // The `clone()` method from Java is handled by `#[derive(Clone)]`.
}

// Provide access to AbstractDefinition fields and EventFluxElement fields
// These are useful for treating StreamDefinition polymorphically if needed.
impl AsRef<AbstractDefinition> for StreamDefinition {
    fn as_ref(&self) -> &AbstractDefinition {
        &self.abstract_definition
    }
}

impl AsMut<AbstractDefinition> for StreamDefinition {
    fn as_mut(&mut self) -> &mut AbstractDefinition {
        &mut self.abstract_definition
    }
}

// Through AbstractDefinition, can access EventFluxElement
use crate::query_api::eventflux_element::EventFluxElement;
impl AsRef<EventFluxElement> for StreamDefinition {
    fn as_ref(&self) -> &EventFluxElement {
        // Accessing EventFluxElement composed within AbstractDefinition
        self.abstract_definition.as_ref()
    }
}

impl AsMut<EventFluxElement> for StreamDefinition {
    fn as_mut(&mut self) -> &mut EventFluxElement {
        self.abstract_definition.as_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_api::definition::attribute::{Attribute, Type as AttributeType};

    #[test]
    fn test_stream_definition_creation_and_attributes() {
        let stream_def = StreamDefinition::new("InputStream".to_string())
            .attribute("userID".to_string(), AttributeType::STRING)
            .attribute("value".to_string(), AttributeType::INT);

        assert_eq!(stream_def.abstract_definition.get_id(), "InputStream");

        let attributes = stream_def.abstract_definition.get_attribute_list();
        assert_eq!(attributes.len(), 2);

        assert_eq!(attributes[0].get_name(), "userID");
        assert_eq!(attributes[0].get_type(), &AttributeType::STRING);

        assert_eq!(attributes[1].get_name(), "value");
        assert_eq!(attributes[1].get_type(), &AttributeType::INT);

        // Also check default EventFluxElement from composed AbstractDefinition
        assert_eq!(
            stream_def
                .abstract_definition
                .eventflux_element
                .query_context_start_index,
            None
        );
    }

    #[test]
    fn test_stream_definition_id_factory() {
        let stream_def = StreamDefinition::id("MyStream".to_string());
        assert_eq!(stream_def.abstract_definition.get_id(), "MyStream");
    }

    #[test]
    fn test_stream_definition_annotations() {
        use crate::query_api::annotation::Annotation; // Assuming Annotation is defined
        let annotation = Annotation::new("TestAnnotation".to_string());
        let stream_def =
            StreamDefinition::new("AnnotatedStream".to_string()).annotation(annotation.clone()); // Assuming Annotation has clone

        assert_eq!(stream_def.abstract_definition.annotations.len(), 1);
        if let Some(ann) = stream_def.abstract_definition.annotations.first() {
            // Assuming Annotation has a get_name() or public name field
            // Let's assume Annotation has a name field for now or a getter
            // For this test, I'll need to check annotation.rs to confirm.
            // If Annotation::name is private, this test would need adjustment or Annotation would need a getter.
            // Based on previous work, Annotation has a public 'name' field.
            assert_eq!(ann.name, "TestAnnotation");
        } else {
            panic!("Annotation not found");
        }
    }

    #[test]
    fn test_stream_definition_with_config() {
        use crate::core::config::stream_config::{FlatConfig, PropertySource};

        let mut config = FlatConfig::new();
        config.set("type", "source", PropertySource::SqlWith);
        config.set("extension", "timer", PropertySource::SqlWith);
        config.set("timer.interval", "5000", PropertySource::SqlWith);

        let stream_def = StreamDefinition::new("TimerInput".to_string())
            .attribute("tick".to_string(), AttributeType::LONG)
            .with_config(config.clone());

        assert!(stream_def.with_config.is_some());
        let stored_config = stream_def.with_config.as_ref().unwrap();
        assert_eq!(stored_config.get("type"), Some(&"source".to_string()));
        assert_eq!(stored_config.get("extension"), Some(&"timer".to_string()));
        assert_eq!(
            stored_config.get("timer.interval"),
            Some(&"5000".to_string())
        );
    }

    #[test]
    fn test_stream_definition_without_config() {
        let stream_def = StreamDefinition::new("InternalStream".to_string())
            .attribute("value".to_string(), AttributeType::INT);

        assert!(stream_def.with_config.is_none());
        assert!(stream_def.get_with_config().is_none());
    }

    #[test]
    fn test_stream_definition_get_with_config() {
        use crate::core::config::stream_config::{FlatConfig, PropertySource};

        let mut config = FlatConfig::new();
        config.set("extension", "kafka", PropertySource::SqlWith);

        let stream_def = StreamDefinition::new("KafkaInput".to_string()).with_config(config);

        let retrieved_config = stream_def.get_with_config();
        assert!(retrieved_config.is_some());
        assert_eq!(
            retrieved_config.unwrap().get("extension"),
            Some(&"kafka".to_string())
        );
    }

    #[test]
    fn test_stream_definition_take_with_config() {
        use crate::core::config::stream_config::{FlatConfig, PropertySource};

        let mut config = FlatConfig::new();
        config.set("extension", "http", PropertySource::SqlWith);

        let mut stream_def = StreamDefinition::new("HttpSink".to_string()).with_config(config);

        assert!(stream_def.with_config.is_some());

        let taken_config = stream_def.take_with_config();
        assert!(taken_config.is_some());
        assert_eq!(
            taken_config.unwrap().get("extension"),
            Some(&"http".to_string())
        );

        // After taking, with_config should be None
        assert!(stream_def.with_config.is_none());
        assert!(stream_def.take_with_config().is_none());
    }

    #[test]
    fn test_stream_definition_clone_with_config() {
        use crate::core::config::stream_config::{FlatConfig, PropertySource};

        let mut config = FlatConfig::new();
        config.set("extension", "timer", PropertySource::SqlWith);
        config.set("timer.interval", "1000", PropertySource::SqlWith);

        let stream_def = StreamDefinition::new("Original".to_string()).with_config(config);

        let cloned = stream_def.clone();

        assert_eq!(stream_def, cloned);
        assert!(cloned.with_config.is_some());
        assert_eq!(
            cloned.with_config.as_ref().unwrap().get("extension"),
            Some(&"timer".to_string())
        );
        assert_eq!(
            cloned.with_config.as_ref().unwrap().get("timer.interval"),
            Some(&"1000".to_string())
        );
    }

    #[test]
    fn test_stream_definition_builder_chain_with_config() {
        use crate::core::config::stream_config::{FlatConfig, PropertySource};

        let mut config = FlatConfig::new();
        config.set("type", "source", PropertySource::SqlWith);
        config.set("extension", "timer", PropertySource::SqlWith);

        // Test builder pattern works in any order
        let stream_def = StreamDefinition::new("TestStream".to_string())
            .attribute("tick".to_string(), AttributeType::LONG)
            .with_config(config.clone())
            .attribute("name".to_string(), AttributeType::STRING);

        assert_eq!(stream_def.abstract_definition.get_id(), "TestStream");
        assert_eq!(stream_def.abstract_definition.attribute_list.len(), 2);
        assert!(stream_def.with_config.is_some());
        assert_eq!(
            stream_def.with_config.as_ref().unwrap().get("extension"),
            Some(&"timer".to_string())
        );
    }
}
