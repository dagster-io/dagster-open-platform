from dagster import ConfigurableResource


class SequelResource(ConfigurableResource):
    client_id: str
    client_secret: str
    company_id: str
    audience: str = "https://www.introvoke.com/api"
