import uuid
import logging

import pandera as pa
import snowflake.connector

from typing import Iterator

from pandera.typing import DataFrame
from snowflake.connector.connection import SnowflakeConnection
from pyapacheatlas.core import PurviewClient, AtlasProcess, EntityTypeDef
from pyapacheatlas.auth import ServicePrincipalAuthentication

from models.exceptions import SnowflakeLineageQueryExeption
from models.snowflake_dependencies import ObjectDependency, SnowflakeObjectsDependencies
from settings import PurviewSettings, get_purview_settings
from settings import SnowflakeSettings, get_snowflake_settings


def create_purview_client(purview_settings: PurviewSettings) -> PurviewClient:
    """Create Purview client using Azure 
    Service Principal Authentication (client id, client secret)

    Args:
        purview_settings (PurviewSettings): pydantic-settings object for varenv extracts

    Returns:
        pyapacheatlas.core.PurviewClient: Purview client for bulk requests
    """
    azure_auth = ServicePrincipalAuthentication(
        tenant_id= purview_settings.tenant_id,
        client_id=purview_settings.client_id,
        client_secret=purview_settings.client_secret
    )
    
    return PurviewClient(account_name=purview_settings.account, authentication=azure_auth)


def create_snowflake_connection(snowflake_settings: SnowflakeSettings) -> SnowflakeConnection:
    return snowflake.connector.connect(
        user=snowflake_settings.user,
        password=snowflake_settings.password,
        account=snowflake_settings.account,
        warehouse=snowflake_settings.warehouse
    )


def extract_object_dependancies_from_snowflake(snowflake_connection: SnowflakeConnection) -> DataFrame[SnowflakeObjectsDependencies]:
    """Extract and validate schema of Snowflake dependencies from Snowflake view

    Args:
        snowflake_connection (SnowflakeConnection): connection to snowflake account

    Raises:
        SnowflakeLineageQueryExeption: custom query exception

    Returns:
        DataFrame[SnowflakeObjectsDependencies]: Pandas Dataframe of view query results 
    """
    try:
        cursor = snowflake_connection.cursor()
        objects_dependencies = (
            cursor
            .execute("""
select * 
from "PRD_RAW"."PURVIEW"."BLOB_AND_TABLE_MAPPING_VW"
where (referencing_object_name LIKE 'REFERENTIAL_VIDEO' OR referenced_object_name LIKE 'REFERENTIAL_VIDEO');
""")
            .fetch_pandas_all()
        )
        objects_dependencies["snowflake_host"] = snowflake_connection.host
        snowflake_objects_dependencies = SnowflakeObjectsDependencies.validate(objects_dependencies)
    except pa.errors.SchemaError as schema_err:
        message = (
            f"Dataframe validation error check if snowflake "
            f"lineage view has the correct schema, cause: {schema_err} "
        )
        raise SnowflakeLineageQueryExeption(message)
    except Exception as generic_error:
        raise SnowflakeLineageQueryExeption(f"Snowflake query error, cause: {generic_error}")
    else:
        return snowflake_objects_dependencies
    finally:
        cursor.close()
        snowflake_connection.close()


def entity_exists(purview_client: PurviewClient, entity: ObjectDependency | AtlasProcess) -> bool:
    """Check if entity already ingested in Purview via Purview scans

    Args:
        purview_client (PurviewClient): Purview client via pyapacheatlas
        entity_qualified_name (str): business key for atlas entity
        entity_type (str): apache atlas entity type  

    Returns:
        bool: if entity exists in Purview
    """
    match(entity):
        case ObjectDependency():
            return len(purview_client.get_entity(qualifiedName=entity.qualified_name, typeName=entity.type_name)) > 0
        case AtlasProcess():
            return len(purview_client.get_entity(qualifiedName=entity.qualifiedName, typeName=entity.typeName)) > 0
        case _:
            raise ValueError(f"Entity of type {type(entity)} not applicable to purview type entity check")


def transform_snowflake_dependancies_to_atlas_entity(purview_client: PurviewClient, object_deps: DataFrame[SnowflakeObjectsDependencies]) -> Iterator[AtlasProcess]:
    """Check existance of references Purview entity and transform object dependencies to Purview Apache Atlas Process

    Args:
        purview_client (PurviewClient): purview client via autenticated api to test entity existance in purview account
        object_deps (DataFrame[SnowflakeObjectsDependencies]): Dataframe of snowflake objects dependencies

    Yields:
        Iterator[AtlasProcess]: Iterator of AtlasProcess objects
    """
    logger = logging.getLogger("etl_lineage_snowflake_purview")
    # Iterate over Snowflake view objects dependecies
    for _, line in object_deps.iterrows():
        referenced_entity = (
            ObjectDependency(
                database=line["REFERENCED_DATABASE"],
                schema=line["REFERENCED_SCHEMA"],
                name=line["REFERENCED_OBJECT_NAME"],
                snowflake_type=line["REFERENCED_OBJECT_DOMAIN"],
                snowflake_server=line["snowflake_host"]
            )
        )
        # If referenced entity does not exists in Purview (has not already been scanned by Purview)
        # don t create process
        logger.debug(f"Source entity queried from Snowflake: {referenced_entity}")
        if not entity_exists(purview_client, referenced_entity):
            logger.info(f"Entity with qualified name {referenced_entity.qualified_name} already exists in Purview")
            continue
        
        referencing_entity = (
            ObjectDependency(
                database=line["REFERENCING_DATABASE"],
                schema=line["REFERENCING_SCHEMA"],
                name=line["REFERENCING_OBJECT_NAME"],
                snowflake_type=line["REFERENCING_OBJECT_DOMAIN"],
                snowflake_server=line["snowflake_host"]
            )
        )
        # If referencing entity does not exists in Purview (has not already been scanned by Purview)
        # don t create process
        logger.debug(f"Target entity queried from Snowflake: {referencing_entity}")
        if not entity_exists(purview_client, referencing_entity):
            logger.info(f"Entity with qualified name {referencing_entity.qualified_name} already exists in Purview")
            continue

        lineage_process_name = f"Snowflake custom ingestion from {referenced_entity} to {referencing_entity}"
        lineage_process_qual_name = f"snowflake_query_from_{referenced_entity.qualified_name}_to_{referencing_entity.qualified_name}"
        process_type = "SnowflakeStageLoadProcess" if (referenced_entity.snowflake_type == "STAGE") or (referencing_entity.snowflake_type == "STAGE") else "Process"
        lineage_process = (
            AtlasProcess(
                name=lineage_process_name,
                typeName=process_type,
                qualified_name=lineage_process_qual_name,
                inputs=[referenced_entity.to_atlas_entity()],
                outputs=[referencing_entity.to_atlas_entity()],
                guid=f"-{str(uuid.uuid4())}"
            )
        )
        logger.debug(f"Generated lineage entity from source and target: {lineage_process}")
        if not entity_exists(purview_client, lineage_process):
            logger.info(f"Process with qualified name {lineage_process.qualifiedName} dont' exist in Purview")
            yield lineage_process
        else:
            logger.info(f"Process with qualified name {lineage_process.qualifiedName} already exists in Purview")


def load_atlas_objects_to_purview(purview_client: PurviewClient, purview_objects: list[AtlasProcess]):
    """Bulk load many Apache Processes to Purview via autentication request

    Args:
        purview_client (PurviewClient): purview client for request autentication
        purview_objects (list[AtlasProcess]): list of atlas process to send to Purview
    """
    logger = logging.getLogger("etl_lineage_snowflake_purview")
    logger.debug(f"Sending {len(purview_objects)} Atlas Process to Purview")
    if len(purview_objects) == 0:
        logger.info("No new entity to send at Purview")
    else:
        try:
            purview_client.upload_entities(batch=purview_objects, batch_size=len(purview_objects))
        except Exception:
            raise
        else:
            logger.info(f"Sucessfully sent {len(purview_objects)} Atlas Process to Purview")


def upload_missing_type_purview(purview_client: PurviewClient) -> None:
    """Adding missing Atlas Entity to allow SnowflakeStage to be linked to SnowflakeTable
    The new Atlas Entity "SnowflakeStageLoadProcess" has Process and DataSet super type.
    Upload new entity to Purview via upload http request.
    
    Args:
        purview_client (PurviewClient): Purview client to perform upload request
    """
    logger = logging.getLogger("etl_lineage_snowflake_purview")
    try:
        procType = EntityTypeDef(
            "SnowflakeStageLoadProcess",
            superTypes=["DataSet","Process"],
        )
    
        purview_client.upload_typedefs(entityDefs=[procType], force_update=True)
    except Exception:
        raise
    else:
        logger.info("New Purview type SnowflakeStageLoadProcess created")



def etl() -> None:
    """
    etl main process
    """
    logging.basicConfig(level=logging.ERROR)
    logger = logging.getLogger("etl_lineage_snowflake_purview")
    logger.setLevel(logging.DEBUG)
    purview_settings = get_purview_settings()
    snowflake_settings = get_snowflake_settings()    
    purview_client = create_purview_client(purview_settings)
    upload_missing_type_purview(purview_client)
    snowflake_connection = create_snowflake_connection(snowflake_settings)
    rows = extract_object_dependancies_from_snowflake(snowflake_connection)
    entities = list(transform_snowflake_dependancies_to_atlas_entity(purview_client, rows))

    load_atlas_objects_to_purview(purview_client, entities)


if __name__ == "__main__":
    etl()