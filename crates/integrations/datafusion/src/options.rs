// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;

use datafusion::catalog::Session as DFSession;
use iceberg::SessionContext;
use iceberg::sensitive::SensitiveString;

/// Iceberg-specific DataFusion options.
///
/// It does deliberately not implement [`ExtensionOptions`](datafusion::config::ExtensionOptions)
/// and [`ConfigExtension`](datafusion::config::ConfigExtension) to avoid
/// plain-string handling of credential values and prevent SQL users from
/// setting unverified authentication properties such as:
///
/// ```sql
/// SET iceberg.identity = 'alice';
/// ```
#[derive(Clone, Debug, Default)]
pub struct IcebergOptions {
    /// Optional identity used when deriving the Iceberg session context.
    pub identity: Option<String>,

    /// Non-sensitive properties propagated to the Iceberg session context.
    pub properties: HashMap<String, String>,

    /// Sensitive credentials propagated to the Iceberg session context.
    pub credentials: HashMap<String, SensitiveString>,
}

impl IcebergOptions {
    /// Builds a fresh Iceberg [`SessionContext`] from these options.
    ///
    /// Unlike the DataFusion session-derived context used by
    /// [`resolve_session_context`], this generates a new, unique session id
    /// on every call rather than reusing a DataFusion session identifier.
    /// This is intended for query- or session-scoped catalog bindings that
    /// are resolved independently of any particular DataFusion session, for
    /// example one binding created per incoming query.
    pub(crate) fn to_session_context(&self) -> SessionContext {
        build_session_context(self, None)
    }
}

/// Centralizes the mapping from [`IcebergOptions`] to an Iceberg
/// [`SessionContext`], optionally overriding the generated session id.
///
/// When `session_id` is `None`, the underlying builder assigns a fresh,
/// unique id.
fn build_session_context(options: &IcebergOptions, session_id: Option<String>) -> SessionContext {
    let builder = SessionContext::builder()
        .properties(options.properties.clone())
        .credentials(options.credentials.clone());

    match (session_id, &options.identity) {
        (Some(session_id), Some(identity)) => builder
            .session_id(session_id)
            .identity(identity.clone())
            .build(),
        (Some(session_id), None) => builder.session_id(session_id).build(),
        (None, Some(identity)) => builder.identity(identity.clone()).build(),
        (None, None) => builder.build(),
    }
}

/// Derives an Iceberg session context from a DataFusion session and its
/// configured [`IcebergOptions`], if registered.
///
/// The returned context reuses the DataFusion session's id. Query- or
/// session-scoped bindings that are independent of a DataFusion session
/// should use [`IcebergOptions::to_session_context`] instead.
pub(crate) fn resolve_session_context(session: &dyn DFSession) -> Option<SessionContext> {
    let options = session.config().get_extension::<IcebergOptions>()?;
    Some(build_session_context(
        &options,
        Some(session.session_id().to_string()),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::config::SessionConfig;
    use datafusion::prelude::SessionContext as DFSessionContext;

    use super::*;

    #[test]
    fn test_resolve_session_context_distinguishes_missing_and_default_options() {
        let session_without_options = DFSessionContext::new();
        assert!(resolve_session_context(&session_without_options.state()).is_none());

        let config = SessionConfig::new().with_extension(Arc::new(IcebergOptions::default()));
        let session_with_default_options = DFSessionContext::new_with_config(config);
        let context = resolve_session_context(&session_with_default_options.state()).unwrap();

        assert_eq!(
            context.session_id(),
            session_with_default_options.session_id()
        );
        assert!(context.identity().is_none());
        assert!(context.properties().is_empty());
        assert!(context.credentials().is_empty());
    }

    #[test]
    fn test_to_session_context_generates_unique_ids() {
        let options = IcebergOptions {
            identity: Some("query-user".to_string()),
            properties: HashMap::from([("prop".to_string(), "value".to_string())]),
            credentials: HashMap::from([(
                "token".to_string(),
                SensitiveString::from("secret".to_string()),
            )]),
        };

        let first = options.to_session_context();
        let second = options.to_session_context();

        assert_ne!(first.session_id(), second.session_id());
        assert_eq!(first.identity(), Some("query-user"));
        assert_eq!(first.properties(), &options.properties);
        assert_eq!(first.credentials(), &options.credentials);
    }

    #[test]
    fn test_debug_redacts_credentials() {
        let secret = "credential-that-must-not-be-logged";
        let options = IcebergOptions {
            credentials: HashMap::from([(
                "token".to_string(),
                SensitiveString::from(secret.to_string()),
            )]),
            ..Default::default()
        };

        let debug_output = format!("{options:?}");
        assert!(!debug_output.contains(secret));
        assert!(debug_output.contains("[REDACTED]"));
    }
}
