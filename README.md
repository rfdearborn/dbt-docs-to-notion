# dbt-docs-to-notion

A Github action for exporting dbt docs to a Notion database, where they can be conveniently consumed (especially by casual users in your org).

## Output

A Notion database, within a parent page of your choosing, with records like this for each model that contain the same information as dbt docs:
![dbt docs to notion output](https://i.imgur.com/Y1EWj9l.png)

## Usage

### Prerequisites

In advance of using this action, you should:

1. [Create a new integration within your Notion workspace](https://www.notion.so/my-integrations)
2. Create a parent Notion page for the docs database and share it with the integration from above
3. Have your Notion integration token and a working dbt `profiles.yml` accessible to your repo (I'd recommend using [Github's repository secrets](https://docs.github.com/en/actions/security-guides/encrypted-secrets); see example workflow below).

Ideally you should also write descriptions for models and columns as is a [best practice](https://docs.getdbt.com/docs/building-a-dbt-project/documentation#adding-descriptions-to-your-project).

> ❗️ Note: this program assumes schema-defined model and column names to be entirely lowercase.

### Inputs

- `dbt-package`: dbt-bigquery, dbt-postgres, dbt-bigquery==1.0.0, etc. (**required**)
- `dbt-profile-path`: where profile.yml lives (default: `./`)
- `dbt-target`: profile target to use for dbt docs generation (**required**)
- `model-records-to-write`: "all" or "model_name_1 model_name_2 ..." (default: "all")
- `notion-database-name`: what to name the Notion database of dbt models (**required**)
- `notion-parent-id`: Notion page where database of dbt models will be added (**required**)
- `notion-token`: Notion token API for integration to use (pass using secrets) (**required**)

### Post-initialization Touchups

Unfortunately, Notion's API doesn't allow for setting the order of properties or records in a database. Thus, after creating your database, you'll probably want to do some re-arranging (I'd recommend adding a table view to your database's parent page).

### Example workflow

```yaml
name: dbt Docs to Notion

on:
  push:
    branches:
      - master

jobs:
  dbt-docs-to-notion:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v3
      - name: Create temp dbt profiles
        run: "printf %s \"$DBT_PROFILES\" > ./profiles.yml"
        env:
          DBT_PROFILES: ${{ secrets.DBT_PROFILES }}
      - name: dbt-docs-to-notion
        uses: rfdearborn/dbt-docs-to-notion@v1.0.8
        with:
          dbt-package: 'dbt-bigquery==1.0.0'
          dbt-profile-path: './'
          dbt-target: 'github_actions'
          model-records-to-write: "all"
          notion-database-name: 'dbt Models'
          notion-parent-id: '604ece5b9dca4cdda449abeabef759e8'
          notion-token: '${{ secrets.DBT_DOCS_TO_NOTION_TOKEN }}'
```

## Todo

- Tests
- Visualize models graph
