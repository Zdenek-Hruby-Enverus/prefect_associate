import httpx
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=2, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1))
def fetch_weather(lat: float = 38.9, lon: float = -77.0) -> float:
    logger = get_run_logger()
    
    base_url = "https://api.open-meteo.com/v1/forecast/"
    temps = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    forecasted_temp = float(temps.json()["hourly"]["temperature_2m"][0])
    logger.info(f"Forecasted temp C: {forecasted_temp} degrees")
    return forecasted_temp

@task(retries=2)
def save_weather(forecasted_temp) -> str:
    logger = get_run_logger()

    with open("weather_result.csv", "w+") as w:
        w.write(str(forecasted_temp))
    
    logger.info("Succcesfully saved temperature")
    return "Successful"


@task
def report(temp):
    markdown_report = f"""# Weather Report
    
## Recent weather

| Time        | Temperature |
|:--------------|-------:|
| Temp Forecast  | {temp} |
"""
    create_markdown_artifact(
        key="weather-report",
        markdown=markdown_report,
        description="Very scientific weather report",
    )


@flow
def pipeline_2tasks():
    logger = get_run_logger()

    forecasted_temp = fetch_weather(lat=10.0, lon=34.0)
    report(temp=forecasted_temp)

    saved_result = save_weather(forecasted_temp=forecasted_temp)
    logger.info("Result successfully saved.")
    return saved_result


if __name__ == "__main__":
    pipeline_2tasks.serve(name='artifacts testing', cron='50 * * * *', tags=["TEST", "ARTIFACTS"])
    # pipeline_2tasks()
