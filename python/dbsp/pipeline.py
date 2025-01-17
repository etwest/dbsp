import uuid
import dbsp_api_client
import yaml
import sys
from typing import Dict, Any
import time

from dbsp_api_client.models.pipeline_config import PipelineConfig
from dbsp_api_client.models.pipeline_config_inputs import PipelineConfigInputs
from dbsp_api_client.models.pipeline_config_outputs import PipelineConfigOutputs
from dbsp_api_client.models.new_pipeline_request import NewPipelineRequest
from dbsp_api_client.models.transport_config import TransportConfig
from dbsp_api_client.models.format_config import FormatConfig
from dbsp_api_client.models.kafka_input_config import KafkaInputConfig
from dbsp_api_client.models.kafka_output_config import KafkaOutputConfig
from dbsp_api_client.models.file_input_config import FileInputConfig
from dbsp_api_client.models.file_output_config import FileOutputConfig
from dbsp_api_client.models.new_pipeline_request import NewPipelineRequest
from dbsp_api_client.models.update_pipeline_request import UpdatePipelineRequest
from dbsp_api_client.models.attached_connector import AttachedConnector
from dbsp_api_client.models.pipeline_status import PipelineStatus
from dbsp_api_client.models.pipeline_descr import PipelineDescr
from dbsp_api_client.api.pipeline import new_pipeline
from dbsp_api_client.api.pipeline import update_pipeline
from dbsp_api_client.api.pipeline import pipeline_stats
from dbsp_api_client.api.pipeline import pipeline_status
from dbsp_api_client.api.pipeline import pipeline_delete
from dbsp_api_client.api.pipeline import pipeline_action
from dbsp.program import DBSPProgram
from dbsp.error import TimeoutException
from dbsp.connector import DBSPConnector


class DBSPPipelineConfig:
    """Pipeline configuration specified by the user when creating
    a new pipeline instance."""

    def __init__(self, project: DBSPProgram, workers: int, name: str = '<anon>', description: str = ''):
        self.project = project
        self.api_client = self.project.api_client
        self.pipeline_config = PipelineConfig(
            workers=workers,
            inputs=PipelineConfigInputs(),
            outputs=PipelineConfigOutputs()
        )
        self.pipeline_id = None
        self.pipeline_version = None
        self.attached_connectors = []
        self.status = None
        self.name = name
        self.description = description

    def add_input(self, stream: str, connector: DBSPConnector):
        """Add an input endpoint to the pipeline configuration.

        Args:
            stream (str): Input name (what table to connect to).
            connector (DBSPConnector): Connector configuration.
        """
        connector.save()

        self.attached_connectors.append(AttachedConnector(
            connector_id=connector.connector_id,
            is_input=True,
            name=uuid.uuid4().hex,
            config=stream,
        ))

    def add_kafka_input(self, name: str, stream: str, config: KafkaInputConfig, format: FormatConfig):
        """Add an input connector that reads data from Kafka to the pipeline configuration.

        Args:
            name (str): Name of the input connector.
            stream (str): The table name to connect to.
            config (KafkaInputConfig): Config for the new Kafka connector.
            format (FormatConfig): Data format specification, e.g., CsvInputFormatConfig().
        """
        self.add_input(
            stream,
            DBSPConnector(self.api_client, name, TransportConfig(
                name="kafka",
                config=config), format=format))

    def add_http_input(self, stream: str, name: str, format: FormatConfig):
        """Add an HTTP input endpoint

        Args:
            stream (str): Input stream name to connect the endpoint to.
            format_ (FormatConfig): Data format specification, e.g., CsvInputFormatConfig().
        """
        self.add_input(
            stream,
            DBSPConnector(self.api_client, name, TransportConfig(
                name="http"), format=format))

    def add_kafka_output(self, name: str, stream: str, config: KafkaOutputConfig, format: FormatConfig):
        """Add a Kafka output connector to the pipeline configuration.

        Args:
            name (str): Name of the output connector.
            stream (str): The view which produces the output for the connector.
            config (KafkaOutputConfig): Config for the new Kafka connector.
            format (FormatConfig): Data format specification, e.g., CsvInputFormatConfig().
        """
        self.add_output(
            stream,
            DBSPConnector(self.api_client, name, TransportConfig(
                name="kafka",
                config=config), format=format))

    def add_output(self, stream: str, connector: DBSPConnector):
        """Add an output connector to the pipeline configuration.

        Args:
            stream (str): What view to connect to the endpoint.
            connector (DBSPConnector): Connector configuration.
        """
        connector.save()

        self.attached_connectors.append(AttachedConnector(
            connector_id=connector.connector_id,
            is_input=False,
            name=uuid.uuid4().hex,
            config=stream,
        ))

    def add_file_input(self, stream: str, filepath: str, format: FormatConfig):
        """Add an input connector that reads data from a file to the pipeline configuration.

        Args:
            stream (str): Input table the connector is connected to.
            filepath (str): File to read data from.
            format (FormatConfig): Data format specification, e.g., CsvInputFormatConfig().
        """
        self.add_input(
            stream,
            DBSPConnector(self.api_client, filepath, TransportConfig(
                name="file",
                config=FileInputConfig.from_dict(dict({'path': filepath}))), format=format))

    def add_file_output(self, stream: str, filepath: str, format: FormatConfig):
        """Add an output connector that reads data from a file to the pipeline configuration.

        Args:
            stream (str): What view the connector is connected to.
            filepath (str): File to write to.
            format (FormatConfig): Data format specification, e.g., CsvOutputFormatConfig().
        """
        self.add_output(
            stream,
            DBSPConnector(self.api_client, filepath, TransportConfig(
                name="file",
                config=FileOutputConfig.from_dict(dict({'path': filepath}))), format=format))

    def add_http_output(self, stream: str, name: str, format: FormatConfig):
        """Add an HTTP output endpoint

        Args:
            stream (str): Output stream name to connect the endpoint to.
            format_ (FormatConfig): Data format specification, e.g., CsvOutputFormatConfig().
        """
        self.add_output(
            stream,
            DBSPConnector(self.api_client, name, TransportConfig(
                name="http"), format=format))



    def yaml(self) -> str:
        """Convert pipeline configuration to YAML format."""
        config = self.pipeline_config.to_dict().copy()
        del config['inputs']
        del config['outputs']
        return yaml.dump(config)

    def save(self):
        "Save the pipeline configuration to DBSP."
        # print("yaml:\n" + self.yaml())
        if self.pipeline_id == None:
            body = NewPipelineRequest(
                program_id=self.project.program_id,
                name=self.name,
                description=self.description,
                config=self.yaml(),
                connectors=self.attached_connectors,
            )
            response = new_pipeline.sync_detailed(client=self.api_client, json_body=body).unwrap(
                "Failed to create pipeline config")
            self.pipeline_id = response.pipeline_id
            self.pipeline_version = response.version
        else:
            body = UpdatePipelineRequest(
                pipeline_id=self.pipeline_id,
                program_id=self.project.program_id,
                name=self.name,
                description=self.description,
                config=self.yaml(),
                connectors=self.attached_connectors,
            )
            response = update_pipeline.sync_detailed(
                client=self.api_client, json_body=body).unwrap("Failed to update pipeline config")
            self.pipeline_version = response.version

    def run(self):
        """Launch a new pipeline.

        Create and run a new pipeline for the specified configuration.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.
        """
        self.save()
        pipeline_action.sync_detailed(
            client=self.api_client, pipeline_id=self.pipeline_id, action='deploy').unwrap("Failed to deploy pipeline")
        self.status = PipelineStatus.DEPLOYED
        pipeline_action.sync_detailed(
            client=self.api_client, pipeline_id=self.pipeline_id, action='start').unwrap("Failed to start pipeline")
        self.status = PipelineStatus.RUNNING

    def pause(self):
        """Pause pipeline.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.
        """
        pipeline_action.sync_detailed(
            client=self.api_client, pipeline_id=self.pipeline_id, action='pause').unwrap("Failed to pause pipeline")
        self.status = PipelineStatus.PAUSED

    def start(self):
        """Resume a paused pipeline.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.
        """
        pipeline_action.sync_detailed(
            client=self.api_client, pipeline_id=self.pipeline_id, action='start').unwrap("Failed to start pipeline")
        self.status = PipelineStatus.RUNNING

    def wait(self, timeout: float = sys.maxsize):
        """Wait for the pipeline to process all inputs to completion.

        This method should only be used for pipelines configured with finite input streams, e.g., files.

        Raises:
            httpx.TimeoutException: If the DBSP server takes longer than Client.timeout to reply to a request.
            dbsp.DBSPServerError: If the DBSP server returns an error.
            dbsp.TimeoutException: If the pipeline does not terminate within 'timeout' seconds.
        """
        start = time.time()
        while time.time() - start < timeout:
            status = self.stats()
            if status['global_metrics']['pipeline_complete'] == True:
                return
            time.sleep(0.5)
        raise TimeoutException(
            "Timeout waiting for the pipeline to complete after " + str(timeout) + "s")


    def run_to_completion(self, timeout: float = sys.maxsize):
        """Launch a new pipeline, wait for it to run to completion, and delete the pipeline.

        This method should only be used for pipelines configured with finite input streams, e.g., files.

        Raises:
            httpx.TimeoutException: If the DBSP server takes longer than Client.timeout to reply to a request.
            dbsp.DBSPServerError: If the DBSP server returns an error.
            dbsp.TimeoutException: If the pipeline does not terminate within 'timeout' seconds.
        """
        self.run()
        try:
            self.wait(timeout)
        except TimeoutException as e:
            self.delete()
            raise
        self.delete()

    def descriptor(self) -> PipelineDescr:
        """Retrieve pipeline descriptor.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.
        """
        desc = pipeline_status.sync_detailed(client=self.api_client, id=self.pipeline_id).unwrap(
            "Failed to retrieve pipeline metadata")
        return desc

    def stats(self) -> Dict[str, Any]:
        """Retrieve pipeline status and performance counters.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.
        """
        status = pipeline_stats.sync_detailed(
            client=self.api_client, pipeline_id=self.pipeline_id).unwrap("Failed to retrieve pipeline status")
        return status.additional_properties

    def delete(self):
        """Terminate and delete a pipeline.

        Shut down the pipeline if it is still running and delete it from
        the server.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.
        """
        pipeline_delete.sync_detailed(
            client=self.api_client, pipeline_id=self.pipeline_id).unwrap("Failed to delete pipeline")
        self.pipeline_id = None
