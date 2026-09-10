from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.zones import ZoneSelection, resolve_zones


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    # Rollout / Zonewise control
    enabled_zones: str = "ALL"

    @field_validator("enabled_zones")
    @classmethod
    def validate_enabled_zones(cls, v: str) -> str:
        selection = resolve_zones(v)
        if selection.mode == "ALL":
            return "ALL"
        return ",".join(selection.zone_codes)

    @property
    def zone_selection(self) -> ZoneSelection:
        return resolve_zones(self.enabled_zones)

    # Pyro API
    pyro_base_url: str
    pyro_api_key: str
    pyro_login_id: str
    pyro_password: str
    # pyro_mpin: str
    pyro_secret_key: str
    pyro_request_timeout_seconds: float = 30.0
    admin_api_key: str = ""
    # Oracle DB (BCD table only)
    oracle_user: str
    oracle_password: str
    oracle_dsn: str              # host:port/service_name

    # Postgres DB. Legacy PG_* names remain accepted for the write endpoint.
    pg_write_host: str = Field(validation_alias=AliasChoices("PG_WRITE_HOST", "PG_HOST"))
    pg_write_port: int = Field(default=5432, validation_alias=AliasChoices("PG_WRITE_PORT", "PG_PORT"))
    pg_write_database: str = Field(validation_alias=AliasChoices("PG_WRITE_DATABASE", "PG_DATABASE"))
    pg_write_user: str = Field(validation_alias=AliasChoices("PG_WRITE_USER", "PG_USER"))
    pg_write_password: str = Field(validation_alias=AliasChoices("PG_WRITE_PASSWORD", "PG_PASSWORD"))
    pg_read_host: str = Field(validation_alias="PG_READ_HOST")
    pg_read_port: int = Field(default=5433, validation_alias="PG_READ_PORT")
    pg_read_database: str = Field(validation_alias="PG_READ_DATABASE")
    pg_read_user: str = Field(validation_alias="PG_READ_USER")
    pg_read_password: str = Field(validation_alias="PG_READ_PASSWORD")
    pg_min_conn: int = 2
    pg_max_conn: int = 10
    recharge_max_retries: int = 3
    # Deployment
    callback_base_url: str

    # Scheduler
    enable_scheduler: bool = True    
    oracle_batch_fetch_size: int = 500
    status_check_max_attempts: int = 5
    # Recharge processing
    recharge_batch_size: int = 500
    # Scheduler — auth
    scheduler_auth_hour: int = 0
    scheduler_auth_minute: int = 5

    # Scheduler — intervals (in their natural units)
    scheduler_batch_population_interval_minutes: int = 30
    scheduler_recharge_interval_minutes: int = 15
    scheduler_status_check_interval_minutes: int = 5

    # Scheduler — misfire grace times (seconds)
    scheduler_batch_population_grace_seconds: int = 120
    scheduler_recharge_grace_seconds: int = 60
    scheduler_status_check_grace_seconds: int = 30

    run_batch_on_startup: bool = False
    run_recharge_on_startup: bool = False
    run_debit_on_startup: bool = False
    run_cleanup_on_startup: bool = True

    # Legacy property accessors for scripts and callers expecting settings.pg_*
    @property
    def pg_host(self) -> str:
        return self.pg_write_host

    @property
    def pg_port(self) -> int:
        return self.pg_write_port

    @property
    def pg_database(self) -> str:
        return self.pg_write_database

    @property
    def pg_user(self) -> str:
        return self.pg_write_user

    @property
    def pg_password(self) -> str:
        return self.pg_write_password


settings = Settings()
