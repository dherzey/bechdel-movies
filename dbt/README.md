## Configuring dbt locally with BigQuery

```bash
# install dbt using pip
pip install dbt-bigquery

# initialize dbt project
dbt init
```

Input all the needed details for the project if prompted. Alternatively, we can update our dbt profile under the `profiles.yml` file. For running dbt using docker, check out https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering/docker_setup

## Running dbt CLI with Prefect
This project also uses Prefect to trigger dbt run commands. The primary function can be found in the [trigger_dbt_prefect.py](https://github.com/dherzey/bechdel-movies-project/blob/main/dbt/trigger_dbt_prefect.py). Two deployments will be created in Prefect: (1) for running dbt in the dev environment, and (2) for running dbt in the prod environment (see [profiles.yml](https://github.com/dherzey/bechdel-movies-project/blob/main/dbt/profiles.yml) for target info). For running dbt using Prefect flows, make sure that `trigger-dbt-dev` and `trigger-dbt-prod` already exists in the Prefect deployments (if not, run [create_prefect_deployments.py](https://github.com/dherzey/bechdel-movies-project/blob/main/etl/create_prefect_deployments.py)). Then, start Prefect agent and trigger dbt flow using the following:

```bash
# start Prefect agent (if not yet running)
prefect agent start -q default

# trigger dbt in dev
prefect deployment run dbt-dev-flow/trigger-dbt-dev

# trigger dbt in prod
prefect deployment run dbt-prod-flow/trigger-dbt-prod
```

## Resources
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
