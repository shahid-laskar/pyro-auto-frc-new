from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

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

    # Postgres DB (all other tables)
    pg_host: str
    pg_port: int = 5432
    pg_database: str
    pg_user: str
    pg_password: str
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

settings = Settings()
