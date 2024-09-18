import pandera as pa

from datetime import datetime
from dataclasses import dataclass

from pyapacheatlas.core import AtlasEntity


class SnowflakeObjectsDependencies(pa.DataFrameModel):
    referenced_database: str
    referenced_schema: str
    referenced_object_name: str
    referenced_object_id: str
    referenced_object_domain: str
    referencing_database: str
    referencing_schema: str
    referencing_object_name: str
    referencing_object_id: str
    referencing_object_domain: str
    dependency_type: str
    _snowflake_host: str


@dataclass
class ObjectDependency:
    id: str
    database: str
    schema: str
    name: str
    type: str
    snowflake_server: str = "https"
    
    @property
    def qualified_name(self) -> str:
        return f"snowflake://{self.snowflake_server}/databases/{self.database}/schemas/{self.schema}/table/{self.name}"
    
    def __str__(self) -> str:
        return f"{self.database}.{self.schema}.{self.name}".lower()
    
    def to_atlas_entity(self) -> AtlasEntity:
        guid = f"-{self.id}"
        return AtlasEntity(
            guid=guid,
            name=self.name,
            typeName=f"snowflake_{self.type}".lower().replace(' ', '_').replace("materialized_", ""),
            qualified_name=self.qualified_name,
            createTime=datetime.now().isoformat(),
        )