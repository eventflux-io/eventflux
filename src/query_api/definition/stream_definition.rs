/*
 * Copyright 2025-2026 EventFlux.io
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::query_api::annotation::Annotation;
use crate::query_api::definition::abstract_definition::AbstractDefinition;
use crate::query_api::definition::attribute::{Attribute, Type as AttributeType};

/// A stream with a unique id and a list of attributes.
///
/// # Configuration Storage
///
/// Also holds the properties from the SQL WITH clause, so they travel from parsing to
/// runtime creation and on to the source or sink factory.
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
/// The WITH properties are stored in `with_config`. The runtime later merges them with the
/// YAML config before passing the result to the factory's create_initialized().
#[derive(Clone, Debug, PartialEq, Default)]
pub struct StreamDefinition {
    pub abstract_definition: AbstractDefinition,

    /// Properties from the SQL WITH clause, collected during parsing.
    ///
    /// These win over every other config source when the runtime merges them.
    ///
    /// Priority (highest to lowest):
    /// 1. SQL WITH (this field)
    /// 2. CLI `--set` overrides, already applied to the YAML config
    /// 3. YAML config file
    /// 4. Code defaults
    ///
    /// `None` when the SQL has no WITH clause.
    pub with_config: Option<crate::core::config::stream_config::FlatConfig>,
}

impl StreamDefinition {
    pub fn new(id: String) -> Self {
        StreamDefinition {
            abstract_definition: AbstractDefinition::new(id),
            with_config: None,
        }
    }

    /// Same as `new`. Starts a builder chain.
    pub fn id(stream_id: String) -> Self {
        Self::new(stream_id)
    }

    /// Add an attribute.
    ///
    /// A duplicate name is still added. It only prints a warning to stderr.
    pub fn attribute(mut self, attribute_name: String, attribute_type: AttributeType) -> Self {
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

    /// Set the SQL WITH clause properties, replacing any set before.
    ///
    /// SQL parsing calls this so the runtime can use the properties later.
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

    /// The WITH clause properties, or `None` if there are none.
    pub fn get_with_config(&self) -> Option<&crate::core::config::stream_config::FlatConfig> {
        self.with_config.as_ref()
    }

    /// Mutable access to the WITH clause properties, or `None` if there are none.
    pub fn get_with_config_mut(
        &mut self,
    ) -> Option<&mut crate::core::config::stream_config::FlatConfig> {
        self.with_config.as_mut()
    }

    /// Move the WITH clause properties out and leave `None` behind.
    pub fn take_with_config(&mut self) -> Option<crate::core::config::stream_config::FlatConfig> {
        self.with_config.take()
    }
}

// Access to the shared definition fields and the query positions, for code that works on
// any definition.
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

// The EventFluxElement lives inside AbstractDefinition.
use crate::query_api::eventflux_element::EventFluxElement;
impl AsRef<EventFluxElement> for StreamDefinition {
    fn as_ref(&self) -> &EventFluxElement {
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
    use crate::query_api::definition::attribute::Type as AttributeType;

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

        // The query positions start unset.
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
        use crate::query_api::annotation::Annotation;
        let annotation = Annotation::new("TestAnnotation".to_string());
        let stream_def =
            StreamDefinition::new("AnnotatedStream".to_string()).annotation(annotation.clone());

        assert_eq!(stream_def.abstract_definition.annotations.len(), 1);
        if let Some(ann) = stream_def.abstract_definition.annotations.first() {
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

        // with_config can sit anywhere in the builder chain
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
