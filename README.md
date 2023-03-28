# dbt-decodable

[dbt] adapter for [Decodable].

[dbt] enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

Decodable is a fully managed stream processing service, based on [Apache Flink®] and using SQL as the primary means of defining data streaming pipelines.

## Installation

`dbt-decodable` is available on [PyPI]. To install the latest version via `pip` (optionally using a virtual environment),
run:

```nofmt
python3 -m venv dbt-venv         # create the virtual environment
source dbt-venv/bin/activate     # activate the virtual environment
pip install dbt-decodable        # install the adapter
```

## Getting Started

Once you've installed dbt in a virtual environment, we recommend trying out the example project provided by decodable:

```bash
# clone the example project
git clone https://github.com/decodableco/dbt-decodable.git
cd dbt-decodable/example_project/example/

# Ensure you can connect to decodable via the decodable CLI:
# If you don't have installed the decodable CLI,
# install it following these instructions: https://docs.decodable.co/docs/setup#install-the-cli-command-line-interface
decodable connection list

# Ensure you have a  ~/.dbt/profiles.yml file:
cat ~/.dbt/profiles.yml
dbt-decodable: # this name must match the 'profile' from dbt_project.yml
  outputs:
    dev:
      account_name: <fill in your decodable account name>
      profile_name: default # fill in any profile defined in ~/.decodable/config
      type: decodable
      database: db
      schema: demo
  target: dev

# This will launch the example project
dbt run
```

Note that this dbt adapter ignores the `active-profile` setting in `~/.decodable/config`. You must put the decodable profile you want to use
in the `~/.dbt/profiles.yml` file into the `profile_name` setting.
The adapter does not support a custom decodable `base-url` (e.g. for local development or proxies).

## Configuring your profile

Profiles in dbt describe a set of configurations specific to a connection with the underlying data warehouse. Each dbt project should have a corresponding profile (though profiles can be reused for different project). Within a profile, multiple targets can be described to further control dbt's behavior. For example, it's very common to have a `dev` target for development and a `prod` target for production related configurations.

Most of the profile configuration options available can be found inside the [`dbt documentation`](https://docs.getdbt.com/reference/profiles.yml). Additionally, `dbt-decodable` defines a few adapter-specific ones that can be found below.

```yml
dbt-decodable:        # the name of the profile
  target: dev         # the default target to run commands with
  outputs:            # the list of all defined targets under this profile
    dev:              # the name of the target
      type: decodable
      database: None  # Ignored by this adapter, but required properties
      schema: None    # Ignored by this adapter, but required properties

      # decodable specific settings
      account_name: [your account]          # Decodable account name
      profile_name: [name of the profile]   # Decodable profile name
      materialize_tests: [true | false]     # whether to materialize tests as a pipeline/stream pair, default is `false`
      timeout: [ms]                         # maximum accumulative time a preview request should run for, default is `60000`
      preview_start: [earliest | latest]    # whether preview should be run with `earliest` or `latest` start position, default is `earliest`
      local_namespace: [namespace prefix]   # prefix added to all entities created on Decodable, default is `None`, meaning no prefix gets added.
```

dbt looks for the `profiles.yml` file in the `~/.dbt` directory. This file contains all user profiles.

## Supported Features

### Materializations

Only table [materialization](https://docs.getdbt.com/docs/build/materializations) is supported for dbt models at the moment. A dbt table model translates to a pipeline/stream pair on Decodable, both sharing the same name. Pipelines for models are automatically activated upon materialization.

To materialize your models simply run the [`dbt run`](https://docs.getdbt.com/reference/commands/run) command, which will perform the following steps for each model:

1. Create a stream with the model's name and schema inferred by Decodable from the model's SQL.

2. Create a pipeline that inserts the SQL's results into the newly created stream.

3. Activate the pipeline.

By default, the adapter will not tear down and recreate the model on Decodable if no changes to the model have been detected. Invoking dbt with the `--full-refresh` flag set, or setting that configuration option for a specific model will cause the corresponding resources on Decodable to be destroyed and built from scratch. See the [docs](https://docs.getdbt.com/reference/resource-configs/full_refresh) for more information on using this option.

### Custom model configuration

A `watermark` option can be configured to specify the [watermark](https://docs.decodable.co/docs/streams#managing-streams) to be set for the model's respective Decodable stream. See the [http events example](example_project/example/models/example/http_events.sql).

A `primary_key` option can be configured to specify the primary key if the target stream is a [change stream](https://docs.decodable.co/docs/streams#stream-types). See the [group by example](example_project/example/models/example/http_events_bytes_sent.sql).

More on specifying configuration options per model can be found [here](https://docs.getdbt.com/reference/model-configs).

### Seeds

[`dbt seed`](https://docs.getdbt.com/reference/commands/seed/) will perform the following steps for each specified seed:

1. Create a REST connection and an associated stream with the same name (reflecting the seed's name).

2. Activate the connection.

3. Send the data stored in the seed's `.csv` file to the connection as events.

4. Deactivate the connection.

After these steps are completed, you can access the seed's data on the newly created stream.

### Sources

[`Sources`](https://docs.getdbt.com/docs/build/sources) in dbt correspond to Decodable's source connections. However, `dbt source` command is not supported at the moment.

### Documentation

[`dbt docs`](https://docs.getdbt.com/reference/commands/cmd-docs) is not supported at the moment. You can check your Decodable account for details about your models.

### Testing

Based on the `materialize_tests` option set for the current target, [`dbt test`](https://docs.getdbt.com/reference/commands/test) will behave differently:

* `materialize_tests = false` will cause dbt to run the specified tests as previews return the results after they finish. The exact time the preview runs for, as well as whether they run starting positions should be set to `earliest` or `latest` can be changed using the `timeout` and `preview_start` target configurations respectively.

* `materialize_tests = true` will cause dbt to persist the specified tests as pipeline/stream pairs on Decodable. This configuration is designed to allow continous testing of your models. You can then run a preview on the created stream (for example using [Decodable CLI]) to monitor the results.

### Snapshots

Neither the [`dbt snapshot`] command nor the notion of snapshots are supported at the moment.

### Additional Operations

`dbt-decodable` provides a set of commands for managing the project's resources on Decodable. Those commands can be run using [`dbt run-operation {name} --args {args}`](https://docs.getdbt.com/reference/commands/run-operation).

Example invocation of the `delete_streams` operation detailed below:

```bash
$ dbt run-operation delete_streams --args '{streams: [stream1, stream2], skip_errors: True}'
```

___

#### **`stop_pipelines(pipelines)`**

**pipelines** : Optional list of names. Default value is `None`.

Deactivate pipelines for resources defined within the project. If the `pipelines` arg is provided, the command only considers the listed resources. Otherwise, it deactivates all pipelines associated with the project.

___

#### **`delete_pipelines(pipelines)`**

**pipelines** : Optional list of names. Default value is `None`.

Delete pipelines for resources defined within the project. If the `pipelines` arg is provided, the command only considers the listed resources. Otherwise, it deletes all pipelines associated with the project.

___

#### **`delete_streams(streams, skip_errors)`**

**streams** : Optional list of names. Default value is `None`. <br>
**skip_errors** : Whether to treat errors as warnings. Default value is `true`.

Delete streams for resources defined within the project. Note that it does not delete pipelines associated with those streams, failing to remove a stream if one exists. For a complete removal of stream/pipeline pairs, see the `cleanup` operation. <br>
If the `streams` arg is provided, the command only considers the listed resources. Otherwise, it attempts to delete all streams associated with the project. <br>
If `skip_errors` is set to `true`, failure to delete a stream (e.g. due to an associated pipeline) will be reported as a warning. Otherwise, the operation stops upon the first error encountered.

___

#### **`cleanup(list, models, seeds, tests)`**

**list** : Optional list of names. Default value is `None`. <br>
**models** : Whether to include models during cleanup. Default value is `true`. <br>
**seeds** : Whether to include seeds during cleanup. Default value is `true`. <br>
**tests** : Whether to include tests during cleanup. Default value is `true`.

Delete all Decodable entities resulting from the materialization of the project's resources, i.e. connections, streams and pipelines. <br>
If the `list` arg is provided, the command only considers the listed resources. Otherwise, it deletes all entities associated with the project. <br>
The `models`, `seeds` and `tests` arguments specify whether those resource types should be included in the cleanup. Note that cleanup does nothing for tests that have not been materialized.

## Contributions

Contributions to this repository are more than welcome.
Please create any pull requests against the [main] branch.

Each release is maintained in a `releases/*` branch, such as `releases/v1.3.2`, and there's a tag for it.

### Build local version
```bash
pip install .
```

### How to create a release

This is based on an example release called `v1.3.3`.

```bash
# We assume to be on 'main'.
# Fork into release branch
git checkout -b releases/v1.3.3
# Edit pyproject.toml and set: version = "1.3.3"
vi pyproject.toml
# create release commit
git commit -am "[#2] Set version to v1.3.3"
# Create a release with a tag from the GitHub UI pointing to the commit we just created.
# CI will do the rest.
```

## License

This code base is available under the Apache License, version 2.

Apache Flink, Flink®, Apache®, the squirrel logo, and the Apache feather logo are either registered trademarks or trademarks of The Apache Software Foundation.

[Apache Flink]: https://flink.apache.org/
[dbt]: https://www.getdbt.com/
[Decodable]: https://www.decodable.co/
[Decodable CLI]: https://docs.decodable.co/docs/command-line-interface
[develop]: https://github.com/decodableco/dbt-decodable/tree/develop
[gitflow]: https://nvie.com/posts/a-successful-git-branching-model/
[PyPI]: https://pypi.org/project/dbt-decodable/
