## Running dbt CLI with Prefect
This project uses Prefect to trigger dbt run commands. The Python script for running dbt commands can be found in [trigger_dbt_prefect.py](https://github.com/dherzey/bechdel-movies-project/blob/main/dbt/trigger_dbt_prefect.py). A deployment will be created in Prefect, `trigger-dbt-prod`, for running and testing dbt in the prod environment (see [profiles.yml](https://github.com/dherzey/bechdel-movies-project/blob/main/dbt/profiles.yml) for target details).

```bash
# start Prefect agent (if not yet running)
prefect agent start -q default

# trigger dbt in dev
dbt build --target dev

# trigger dbt in prod
prefect deployment run dbt-prod-flow/trigger-dbt-prod
```

## Resources
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
- For running dbt using docker, check out https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering/docker_setup