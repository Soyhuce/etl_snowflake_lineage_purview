import logging

from pyapacheatlas.core import PurviewClient
from pyapacheatlas.auth import ServicePrincipalAuthentication

from settings import PurviewSettings, get_purview_settings, get_snowflake_settings


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


def delete_all_entities_from_host_name(purview_client: PurviewClient, source_host_name: str) -> None:
    """Delete all entities collected by Purview Scan in Purview Database using search and source host name
    
    Args:
        purview_client (PurviewClient): Purview client from host
        source_host_name (str): Source host name
    """
    logger = logging.getLogger("delete_entities_purview")
    entities_not_found = True
    search = purview_client.discovery.search_entities(source_host_name)
    for entity in search:
        entities_not_found = False
        logger.debug(f"Deleting {entity['entityType']} entity with name: {entity['qualifiedName']}")
        purview_client.delete_entity(guid=entity["id"])
    if entities_not_found:
        logger.warning(f"Any entity from source: {source_host_name} found in Purview") 


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    logger = logging.getLogger("delete_entities_purview")
    logger.setLevel(logging.DEBUG)
    purview_settings = get_purview_settings()
    snowflake_settings = get_snowflake_settings()    
    purview_client = create_purview_client(purview_settings)
    delete_all_entities_from_host_name(purview_client, "https://app.powerbi.com")