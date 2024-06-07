# Data Rafting Kit - A declarative framework for PySpark Pipelines

Data Rafting Kit enables a YAML based approach to PySpark pipelines with integrated features such as data quality checks. Instead of spending time figuring out the syntax or building common PySpark, simply write your pipelines in a YAML format and run via the library. We target the use of Azure based services, primarily Microsoft Fabric and Azure Databricks, but this library can be used in any PySpark environment.

## Why 'Data Rafting Kit'?

Naming a library is hard... In the real world, a raft flows down a stream carrying a load. In our data world, this library carries data along a data stream.

## Build & Installation

- Currently, there is no installation via PIP, but we plan to address this in the future.
- All dependency management is with [Poetry](https://python-poetry.org/). Make sure you have Poetry setup on your system.
- Run `poetry install`, followed by `poetry build` to produce a `.whl` file.
- Upload to your Spark cluster and get building!

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
