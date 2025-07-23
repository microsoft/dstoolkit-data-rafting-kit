# Data Rafting Kit - A declarative framework for PySpark Pipelines

Data Rafting Kit enables a YAML based approach to PySpark pipelines with integrated features such as data quality checks. Instead of spending time figuring out the syntax or building common PySpark, simply write your pipelines in a YAML format and run via the library. We target the use of Azure based services, primarily Microsoft Fabric and Azure Databricks, but this library should be usable in any PySpark environment.

This helper library aims to make it easier to build Fabric based pipelines that interact with Azure services.

A simple pipeline with data quality checks can be defined in a few lines of YAML:

```yaml
name: "My First Data Rafting Kit Pipeline"
env:
  target: fabric

pipeline:
  inputs:
    - type: delta_table
      name: input_delta_table
      params:
        location: './data/netflix_titles'

  transformations:
    - type: filter
      name: filter_movies
      params:
        condition: 'type = "TV Show"'

    - type: anonymize
      name: blank_directors
      params:
        columns:
          - name: 'director'
            type: 'mask'
            chars_to_mask: 5

    - type: config
      name: sub_transformations
      params:
        path: './sub_transformations.yaml'
        arguments:
          select_value: 1

  outputs:
    - type: delta_table
      name: output
      params:
        location: './data/netflix_titles_output'
        mode: merge
        merge_spec:
          condition: 'source.show_id = target.show_id'

  data_quality:
    # For running data quality checks
    - name: check_data
      type: checks
      params:
        mode: flag
        unique_column_identifiers:
          - show_id
        checks:
          - type: expect_column_values_to_be_unique
            params:
              column: show_id
          - type: expect_column_values_to_be_between
            params:
              column: release_year
              min_value: "1900"
              max_value: "2005"

    # For calculating data quality metrics
    - name: calculate_metrics
      type: metrics
      params:
        column_wise: True
        checks:
          - type: expect_column_values_to_be_unique
            params:
              column: show_id
          - type: expect_column_values_to_be_in_set
            params:
              column: type
              values: ['Movies']
          - type: expect_column_values_to_be_between
            params:
              column: release_year
              min_value: "1900"
              max_value: "2005"
```

## Why 'Data Rafting Kit'?

Naming a library is hard... In the real world, a raft flows down a stream carrying a load. In our data world, this library carries data along a data stream making it readily consumable for AI.

### Why a new library?

A declarative approach to pipelines and data pipelines is not a new concept but there is no existing library / framework dedicated to Microsoft Fabric and Azure specific services. The YAML approach in this project aims to replicate a similar style to [Azure DevOps Pipelines](https://learn.microsoft.com/en-us/azure/devops/pipelines/get-started/what-is-azure-pipelines?view=azure-devops) to make it easy to spin up quick pipelines with similar syntax. In this repository, we provide an pipeline implementation library written from scratch with Microsoft Fabric in mind.

*This project is still in early development. The functionality provided may not cover every use case yet.*

## Key Features

- Easy YAML (or JSON) syntax, to quickly build repeatable pipelines
- Inbuilt parametrisation of pipelines and ability to load sub config files with the support of Jinja templating.
- Integrated testing of pipelines for expected behaviour
- Streaming support from EventHub (Kafka Endpoint) or Delta Tables. Includes support to stream to multiple tables.
- Data quality checks, powered by [Great Expectations](https://github.com/great-expectations/great_expectations) (including for streaming data pipelines). Includes the functionality to separate results and calculate data quality metrics:
  - Completeness
  - Validitity
  - Timeliness
  - Uniqueness
  - Integrity
- Anonymisation, powered by [Microsoft Presidio](https://github.com/microsoft/presidio)

## Build & Installation

- Currently, there is no installation via PIP, but we plan to address this in the future.
- All dependency management is with [Poetry](https://python-poetry.org/). Make sure you have Poetry setup on your system.
- Run `poetry install`, followed by `poetry build` to produce a `.whl` file.
- Upload to your Spark cluster and get building!
- A [DevContainer](https://containers.dev/) is provided for easy local development.

## Runtime Dependencies

This project requires Python 3.11 or greater to run. For Spark runtimes, the following minimum version is required:

- Microsoft Fabric: 1.3
- Azure Databricks: 15.1

## Samples

Sample pipelines can be found under `./samples`. We aim to keep these up to date with different options.

## Credit

This project makes key use of some dependencies to power some of the features. These repositories have made implementing this project far easier. Thanks to:

- [Great Expectations](https://github.com/great-expectations/great_expectations)
- [Microsoft Presidio](https://github.com/microsoft/presidio)
- [Pydantic](https://github.com/pydantic/pydantic)

The following abandoned library has provided some initial inspiration for direction of a YAML based approach for data pipelines:

- [DataDuct](https://github.com/coursera/dataduct)

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
