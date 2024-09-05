import uuid

import pandas
import snowflake.connector

from typing import Iterator

from snowflake.connector.connection import SnowflakeConnection
from pyapacheatlas.core import AtlasEntity, PurviewClient, AtlasProcess
from pyapacheatlas.auth import ServicePrincipalAuthentication

from snowflake_dependencies import SnowflakeDependencies
from settings import PurviewSettings, get_purview_settings
from settings import SnowflakeSettings, get_snowflake_settings


def create_purview_client(purview_settings: PurviewSettings) -> PurviewClient:
    try:
        azure_auth = ServicePrincipalAuthentication(
            tenant_id= purview_settings.tenant_id,
            client_id=purview_settings.client_id,
            client_secret=purview_settings.client_secret
        )
        
        purview_client =  PurviewClient(account_name=purview_settings.account, authentication=azure_auth)
    except Exception as err:
        print(err)
    else:
        return purview_client
    

def create_snowflake_connection(snowflake_settings: SnowflakeSettings) -> SnowflakeConnection:
    return snowflake.connector.connect(
        user=snowflake_settings.user,
        password=snowflake_settings.password,
        account=snowflake_settings.account,
        warehouse=snowflake_settings.warehouse
    )


def extract_object_dependancies_from_snowflake(snowflake_connection: SnowflakeConnection) -> pandas.DataFrame:
    cursor = snowflake_connection.cursor()
    objects_dependencies = (
        cursor
        .execute("SELECT * FROM AUDIT.AUDIT_LINEAGE.PURVIEW_LINEAGE_OBJECT_DEPENDENCIES")
        .fetch_pandas_all()
    )
    objects_dependencies["_snowflake_host"] = snowflake_connection.host
    return objects_dependencies


def entity_exists(purview_client: PurviewClient, entity_qualified_name: str, entity_type: str) -> str:
    return len(purview_client.get_entity(qualifiedName=entity_qualified_name, typeName=entity_type)) > 0


def transform_snowflake_dependancies_to_atlas_entity(object_deps: pandas.DataFrame) -> Iterator[AtlasEntity]:
    """
    TODO: comment definir l'id pour reutiliser celui issu des scans

    Args:
        object_deps (pandas.DataFrame): _description_

    Yields:
        Iterator[AtlasEntity]: _description_
    """
    for _, line in object_deps.iterrows():
        referenced_entity = (
            SnowflakeDependencies(
                id=line["REFERENCED_OBJECT_ID"],
                database=line["REFERENCED_DATABASE"],
                schema=line["REFERENCED_SCHEMA"],
                name=line["REFERENCED_OBJECT_NAME"],
                type=line["REFERENCED_OBJECT_DOMAIN"],
                snowflake_server=line["_snowflake_host"]
            )
            .to_atlas_entity()
        )
        
        if not entity_exists(purview_client, referenced_entity.qualifiedName, referenced_entity.typeName):
            yield referenced_entity
        
        
        referencing_entity = (
            SnowflakeDependencies(
                id=line["REFERENCING_OBJECT_ID"],
                database=line["REFERENCING_DATABASE"],
                schema=line["REFERENCING_SCHEMA"],
                name=line["REFERENCING_OBJECT_NAME"],
                type=line["REFERENCING_OBJECT_DOMAIN"],
                snowflake_server=line["_snowflake_host"]
            )
            .to_atlas_entity()
        )
        
        if not entity_exists(purview_client, referencing_entity.qualifiedName, referencing_entity.typeName):
            yield referencing_entity
        
        lineage_process = (
            AtlasProcess(
                name="Snowflake query",
                typeName="Process",
                qualified_name="snowflake://sqlquery",
                inputs=[referenced_entity],
                outputs=[referencing_entity],
                guid=f"-{uuid.uuid4()}"
            )
        )
        
        yield lineage_process


def load_atlas_objects_to_purview(purview_client: PurviewClient, purview_objects: list[AtlasEntity]):
    purview_client.upload_entities(batch=purview_objects, batch_size=len(purview_objects))


if __name__ == "__main__":
    purview_settings = get_purview_settings()
    snowflake_settings = get_snowflake_settings()
    
    purview_client = create_purview_client(purview_settings)
    snowflake_connection = create_snowflake_connection(snowflake_settings)
    rows = extract_object_dependancies_from_snowflake(snowflake_connection)
    entities = list(transform_snowflake_dependancies_to_atlas_entity(rows))
    load_atlas_objects_to_purview(purview_client, entities)