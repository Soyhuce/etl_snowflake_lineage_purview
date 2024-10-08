import pandera as pa

from typing import Optional
from datetime import datetime
from dataclasses import dataclass

from pyapacheatlas.core import AtlasEntity


class SnowflakeObjectsDependencies(pa.DataFrameModel):
    referenced_database: str = pa.Field(alias="REFERENCED_DATABASE", nullable=True)
    referenced_schema: str = pa.Field(alias="REFERENCED_SCHEMA", nullable=True)
    referenced_object_name: str = pa.Field(alias="REFERENCED_OBJECT_NAME", nullable=False)
    referenced_object_domain: str = pa.Field(alias="REFERENCED_OBJECT_DOMAIN", nullable=False)
    referencing_database: str = pa.Field(alias="REFERENCING_DATABASE", nullable=False)
    referencing_schema: str = pa.Field(alias="REFERENCING_SCHEMA", nullable=False)
    referencing_object_name: str = pa.Field(alias="REFERENCING_OBJECT_NAME", nullable=False)
    referencing_object_domain: str = pa.Field(alias="REFERENCING_OBJECT_DOMAIN", nullable=False)
    dependency_type: str = pa.Field(alias="DEPENDENCY_TYPE", nullable=False)
    stage_url: Optional[str] = pa.Field(alias="STAGE_URL", nullable=True)
    snowflake_host: str 


@dataclass
class ObjectDependency:
    database: str
    schema: str
    name: str
    snowflake_type: str
    snowflake_server: str = "https"
    
    @property
    def qualified_name(self) -> str:
        object_name = f"@{self.name}" if self.snowflake_type == "STAGE" else self.name
        type_separator_name = f"{self.snowflake_type.lower()}s"
        return f"snowflake://{self.snowflake_server}/databases/{self.database}/schemas/{self.schema}/{type_separator_name}/{object_name}"
    
    @property
    def type_name(self) -> str:
        return f"snowflake_{self.snowflake_type}".lower().replace(' ', '_').replace("materialized_", "")
            
    def __str__(self) -> str:
        return f"{self.database}.{self.schema}.{self.name}".lower()
    
    def to_atlas_entity(self) -> AtlasEntity:
        return AtlasEntity(
            guid=None,
            name=self.name,
            typeName=self.type_name,
            qualified_name=self.qualified_name,
            createTime=datetime.now().isoformat(),
        )