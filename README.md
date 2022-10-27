# tap-snowflake

`tap-snowflake` is a Singer tap for Snowflake.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

```bash
pipx install git+https://github.com/MeltanoLabs/tap-snowflake.git
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-snowflake --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

Standard `username` and `password` auth is supported.

### Enabling Batch Messaging

This tap is built using the Meltano SDK and therefore supports a `BATCH` [message type](https://sdk.meltano.com/en/latest/batch.html), in
addition to the `RECORD` messages of the Singer spec. This can be enabled either by adding the following to your `config.json`:

```json
{
  // ...
  "batch_config": {
    "encoding": {
      "format": "jsonl",
      "compression": "gzip"
    },
    "storage": {
      "root": "file://tests/core/resources",
      "prefix": "test-batch"
    }
  }
}
```

or its equivalent to your `meltano.yml`

```yaml
config:
  plugins:
    extractors:
      - name: tap-snowflake
        config:
          batch_config:
            encoding:
              format: jsonl
              compression: gzip
            storage:
              root: "file://tests/core/resources"
              prefix: test-batch
```

**Note:** This variant of `tap-snowflake` does not yet support the `INCREMENTAL` replication strategy in `BATCH` mode. Follow [here](https://github.com/meltano/sdk/issues/976#issuecomment-1257848119) for updates.

## Usage

You can easily run `tap-snowflake` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-snowflake --version
tap-snowflake --help
tap-snowflake --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_snowflake/tests` subfolder and
then run:

```bash
poetry run pytest
```

You can also test the `tap-snowflake` CLI interface directly using `poetry run`:

```bash
poetry run tap-snowflake --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-snowflake
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-snowflake --version
# OR run a test `elt` pipeline:
meltano elt tap-snowflake target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
