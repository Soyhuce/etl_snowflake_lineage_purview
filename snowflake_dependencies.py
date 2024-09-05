from datetime import datetime
from dataclasses import dataclass

from pyapacheatlas.core import AtlasEntity


@dataclass
class SnowflakeDependencies:
    id: str
    database: str
    schema: str
    name: str
    type: str
    snowflake_server: str = "https"
    
    @property
    def qualified_name(self) -> str:
        return f"snowflake://{self.snowflake_server}/databases/{self.database}/schemas/{self.schema}/table/{self.name}"
    
    def to_atlas_entity(self) -> AtlasEntity:
        #SALTED_HASH = "snowflake_id_"
        guid = f"-{self.id}"
        return AtlasEntity(
            guid=guid,
            name=self.name,
            typeName=f"snowflake_{self.type}".lower().replace(' ', '_').replace("materialized_", ""),
            qualified_name=self.qualified_name,
            createTime=datetime.now().isoformat(),
        )