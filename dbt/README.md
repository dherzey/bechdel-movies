## Running dbt locally with BigQuery:

```bash
# install dbt using pip
pip install dbt-bigquery

# initialize dbt project
dbt init
```

Input all the needed details for the project if prompted. Alternatively, we can update our dbt profile under the `profiles.yml` file usually located in `~/.dbt/` folder. For running dbt using docker, check out https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering/docker_setup

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
