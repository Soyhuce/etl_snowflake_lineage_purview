from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class SnowflakeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file="snowflake.env", env_prefix='SNOWFLAKE_')
    user: str
    password: str
    account: str
    warehouse: str
    audit_database: str
    audit_schema: str


class PurviewSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file="purview.env", env_prefix='PURVIEW_')
    
    tenant_id: str
    client_id: str
    client_secret: str
    account: str


@lru_cache
def get_purview_settings() -> PurviewSettings:
    return PurviewSettings()

@lru_cache
def get_snowflake_settings() -> SnowflakeSettings:
    return SnowflakeSettings()