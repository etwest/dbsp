use super::{
    ApiPermission, AttachedConnector, ConnectorDescr, ConnectorId, DBError, PipelineDescr,
    PipelineId, PipelineStatus, ProgramDescr, ProgramId, Version,
};
use crate::ProgramStatus;
use anyhow::{anyhow, Result as AnyResult};
use async_trait::async_trait;
use uuid::Uuid;

/// The storage trait contains the methods to interact with the pipeline manager
/// storage layer (e.g., PostgresDB) to implement the public API.
///
/// We use a trait so we can mock the storage layer in tests.
#[async_trait]
pub(crate) trait Storage {
    async fn reset_program_status(&self) -> AnyResult<()>;

    async fn list_programs(&self) -> AnyResult<Vec<ProgramDescr>>;

    /// Retrieve program descriptor.
    ///
    /// Returns a `DBError:UnknownProgram` error if `program_id` is not found in
    /// the database.
    async fn get_program_by_id(&self, program_id: ProgramId) -> AnyResult<ProgramDescr> {
        self.get_program_if_exists(program_id)
            .await?
            .ok_or_else(|| anyhow!(DBError::UnknownProgram(program_id)))
    }

    /// Retrieve program descriptor.
    ///
    /// Returns a `DBError:UnknownName` error if `name` is not found in
    /// the database.
    async fn get_program_by_name(&self, name: &str) -> AnyResult<ProgramDescr> {
        self.lookup_program(name)
            .await?
            .ok_or_else(|| anyhow!(DBError::UnknownName(name.into())))
    }

    /// Validate program version and retrieve program descriptor.
    ///
    /// Returns `DBError::UnknownProgram` if `program_id` is not found in the
    /// database. Returns `DBError::OutdatedProgramVersion` if the current
    /// program version differs from `expected_version`.
    async fn get_program_guarded(
        &self,
        program_id: ProgramId,
        expected_version: Version,
    ) -> AnyResult<ProgramDescr> {
        let descr = self.get_program_by_id(program_id).await?;
        if descr.version != expected_version {
            return Err(anyhow!(DBError::OutdatedProgramVersion(expected_version)));
        }

        Ok(descr)
    }

    /// Queue program for compilation by setting its status to
    /// [`ProgramStatus::Pending`].
    ///
    /// Change program status to [`ProgramStatus::Pending`].
    async fn set_program_pending(
        &self,
        program_id: ProgramId,
        expected_version: Version,
    ) -> AnyResult<()> {
        let descr = self
            .get_program_guarded(program_id, expected_version)
            .await?;

        // Do nothing if the program is already pending (we don't want to bump its
        // `status_since` field, which would move it to the end of the queue) or
        // if compilation is alread in progress.
        if descr.status == ProgramStatus::Pending || descr.status.is_compiling() {
            return Ok(());
        }

        self.set_program_status(program_id, ProgramStatus::Pending)
            .await?;

        Ok(())
    }

    /// Cancel compilation request.
    ///
    /// Cancels compilation request if the program is pending in the queue
    /// or already being compiled.
    async fn cancel_program(
        &self,
        program_id: ProgramId,
        expected_version: Version,
    ) -> AnyResult<()> {
        let descr = self
            .get_program_guarded(program_id, expected_version)
            .await?;

        if descr.status != ProgramStatus::Pending || !descr.status.is_compiling() {
            return Ok(());
        }

        self.set_program_status(program_id, ProgramStatus::None)
            .await?;

        Ok(())
    }

    /// Retrieve code of the specified program along with the program's
    /// meta-data.
    async fn program_code(&self, program_id: ProgramId) -> AnyResult<(ProgramDescr, String)>;

    /// Create a new program.
    async fn new_program(
        &self,
        id: Uuid,
        program_name: &str,
        program_description: &str,
        program_code: &str,
    ) -> AnyResult<(ProgramId, Version)>;

    /// Update program name, description and, optionally, code.
    /// XXX: Description should be optional too
    async fn update_program(
        &self,
        program_id: ProgramId,
        program_name: &str,
        program_description: &str,
        program_code: &Option<String>,
    ) -> AnyResult<Version>;

    /// Retrieve program descriptor.
    ///
    /// Returns `None` if `program_id` is not found in the database.
    async fn get_program_if_exists(&self, program_id: ProgramId)
        -> AnyResult<Option<ProgramDescr>>;

    /// Lookup program by name.
    async fn lookup_program(&self, program_name: &str) -> AnyResult<Option<ProgramDescr>>;

    /// Update program status.
    ///
    /// # Note
    /// - Doesn't check that the program exists.
    /// - Resets schema to null.
    async fn set_program_status(
        &self,
        program_id: ProgramId,
        status: ProgramStatus,
    ) -> AnyResult<()>;

    /// Update program status after a version check.
    ///
    /// Updates program status to `status` if the current program version in the
    /// database matches `expected_version`.
    ///
    /// # Note
    /// This intentionally does not throw an error if there is a program version
    /// mismatch and instead does just not update. It's used by the compiler to
    /// update status and in case there is a newer version it is expected that
    /// the compiler just picks up and runs the next job.
    async fn set_program_status_guarded(
        &self,
        program_id: ProgramId,
        expected_version: Version,
        status: ProgramStatus,
    ) -> AnyResult<()>;

    /// Update program schema.
    ///
    /// # Note
    /// This should be called after the SQL compilation succeeded, e.g., in the
    /// same transaction that sets status to  [`ProgramStatus::CompilingRust`].
    async fn set_program_schema(&self, program_id: ProgramId, schema: String) -> AnyResult<()>;

    /// Delete program from the database.
    ///
    /// This will delete all program configs and pipelines.
    async fn delete_program(&self, program_id: ProgramId) -> AnyResult<()>;

    /// Retrieves the first pending program from the queue.
    ///
    /// Returns a pending program with the most recent `status_since` or `None`
    /// if there are no pending programs in the DB.
    async fn next_job(&self) -> AnyResult<Option<(ProgramId, Version)>>;

    /// Create a new config.
    async fn new_pipeline(
        &self,
        id: Uuid,
        program_id: Option<ProgramId>,
        pipline_name: &str,
        pipeline_description: &str,
        config: &str,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> AnyResult<(PipelineId, Version)>;

    /// Update existing config.
    ///
    /// Update config name and, optionally, YAML.
    async fn update_pipeline(
        &self,
        pipeline_id: PipelineId,
        program_id: Option<ProgramId>,
        pipline_name: &str,
        pipeline_description: &str,
        config: &Option<String>,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> AnyResult<Version>;

    /// Delete config.
    async fn delete_config(&self, pipeline_id: PipelineId) -> AnyResult<()>;

    /// Get input/output status for an attached connector.
    async fn attached_connector_is_input(&self, name: &str) -> AnyResult<bool>;

    async fn set_pipeline_deployed(&self, pipeline_id: PipelineId, port: u16) -> AnyResult<()>;

    /// Set `shutdown` flag to `true`.
    async fn set_pipeline_status(
        &self,
        pipeline_id: PipelineId,
        status: PipelineStatus,
    ) -> AnyResult<bool>;

    /// Delete `pipeline` from the DB.
    async fn delete_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<bool>;

    /// Retrieve pipeline for a given id.
    async fn get_pipeline_by_id(&self, pipeline_id: PipelineId) -> AnyResult<PipelineDescr>;

    /// Retrieve pipeline for a given name.
    async fn get_pipeline_by_name(&self, name: String) -> AnyResult<PipelineDescr>;

    /// List pipelines associated with `program_id`.
    async fn list_pipelines(&self) -> AnyResult<Vec<PipelineDescr>>;

    /// Create a new connector.
    async fn new_connector(
        &self,
        id: Uuid,
        name: &str,
        description: &str,
        config: &str,
    ) -> AnyResult<ConnectorId>;

    /// Retrieve connectors list from the DB.
    async fn list_connectors(&self) -> AnyResult<Vec<ConnectorDescr>>;

    /// Retrieve connector descriptor for the given `connector_id`.
    async fn get_connector_by_id(&self, connector_id: ConnectorId) -> AnyResult<ConnectorDescr>;

    /// Retrieve connector descriptor for the given `name`.
    async fn get_connector_by_name(&self, name: String) -> AnyResult<ConnectorDescr>;

    /// Update existing connector config.
    ///
    /// Update connector name and, optionally, YAML.
    async fn update_connector(
        &self,
        connector_id: ConnectorId,
        connector_name: &str,
        description: &str,
        config: &Option<String>,
    ) -> AnyResult<()>;

    /// Delete connector from the database.
    ///
    /// This will delete all connector configs and pipelines.
    async fn delete_connector(&self, connector_id: ConnectorId) -> AnyResult<()>;

    /// Persist a hash of API key in the database
    async fn store_api_key_hash(
        &self,
        key: String,
        permissions: Vec<ApiPermission>,
    ) -> AnyResult<()>;

    /// Validate an API key against the database
    async fn validate_api_key(&self, key: String) -> AnyResult<Vec<ApiPermission>>;
}
