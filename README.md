# Dagster Open Platform

This repository contains the Dagster Open Platform (DOP) project.

🏗️ **Note:** We are in the early days of moving our internal assets to this repository. Pardon the dust as we continue to build out this repository.

## What is this?

Dagster Open Platform is Dagster Lab's open-source data platform.

This is a full-sized Dagster project that contains **real** assets that are used by the Dagster Labs team. These are assets used for our own analytics and operations. Therefore, if you're a high-growth startup or a budding data team, this should also serve as an amazing reference for what it means to run a data platform at a SaaS business.

We are open-sourcing these assets to provide a reference for how Dagster is used in a real-world setting at the scale of a data platform in a production setting.

## How to use this project

Learning Dagster is not a journey that ends once your first assets are made. There are multiple resources available for you to get started with using Dagster, such as our documentation, quickstarts, and Dagster University. These efforts are amazing ways to get started using Dagster properly and flatten the early learning curve of using Dagster at your organization.

It follows our best practices for how to structure a Dagster project, how to connect with external systems, and how to use Dagster in a production setting.

```bash
.
├── README.md
├── dagster_open_platform
│   ├── __init__.py
│   ├── assets
│   ├── jobs
│   ├── partitions
│   ├── resources
│   ├── schedules
│   ├── sensors
│   └── utils
├── dagster_open_platform_tests
├── pyproject.toml
├── setup.cfg
├── setup.py
└── tox.ini
```

*Note:* This project **does not** include any confidential information about Dagster, environment variables, or configurations for our pipelines. We also excluded assets that contain sensitive business logic. Therefore, this project won't be able to run on your machine without some additional changes.
