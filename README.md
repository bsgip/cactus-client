# CACTUS Client

This is a set of tools for evaluating CSIP-Aus server test procedures defined at [CACTUS Test Definitions](https://github.com/bsgip/cactus-test-definitions).

<img width="1841" height="1003" alt="image" src="https://github.com/user-attachments/assets/0ee5b02e-cb21-476a-a6f2-975f23ecc5ae" />


## Development

`pip install -e .[dev,test]`



## Quickstart

### Installing

CACTUS requires Python 3.12+

Install the latest version from pypi with:
`pip install cactus-client`

If you're looking to update to the latest version:
`pip install --upgrade cactus-client`

To ensure it's installed properly
`cactus --help`


### Working Directory Configuration

CACTUS requires two things:
1. A configuration file - stored either in your home directory or elsewhere (will be created below).
1. A working directory - Where all run outputs will be stored.

**Portable Installation**

If you're trying to keep CACTUS to a single working directory (and want all of your CACTUS operations to run out of that working directory):

1. Create a new empty directory (eg `mkdir cactus-wd`)
1. `cd cactus-wd`
1. `cactus setup -l .`

Please note - all CACTUS commands will now require you to operate out of the `./cactus-wd/` directory
1. `cd cactus-wd`
1. `cactus server`


**Global Installation**

If you'd like your CACTUS commands to work from any directory (but still have the results all stored in the working directory):

1. Create a new empty directory (eg `mkdir cactus-wd`)
1. `cactus setup -g cactus-wd`
1. `cactus server`

### Client/Server Config

Setup the server connections details (dcap refers to your DeviceCapability URI)

1. `cactus server dcap https://your.server/dcap`
2. `cactus server verify true`
3. `cactus server serca path/to/serca.pem`
4. `cactus server notification https://cactus.cecs.anu.edu.au/client-notifications/`
    * Please note - this will utilise the shared, ANU hosted [client-notifications](https://github.com/bsgip/cactus-client-notifications) service
    * If you wish to self host - please see [client-notifications](https://github.com/bsgip/cactus-client-notifications)


Setup your first client - You will be prompted to populate each field (like below)

1. `cactus client myclient1` You should see output like the following

```
Would you like to create a new client with id 'myclient1' [y/n]: y
What sort of client will this act as? [device/aggregator]: device
File path to PEM encoded client certificate: ./testdevice.crt
File path to PEM encoded client key: ./testdevice.key.decrypt
Auto calculate lfdi/sfdi from certificate? [y/n]: y
lfdi=0F3078CFDDAEE28DC20B95635DC116CC2A6D877F
sfdi=40773583337
Client Private Enterprise Number (PEN) (used for mrid generation): 12345
Client PIN (used for matching EndDevice.Registration): 111115
The DERSetting.setMaxW and DERCapability.rtgMaxW value to use (in Watts): 5000
.cactus.yaml has been updated with a new client.
                         myclient1                                                  
┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ key              ┃ value                                         ┃
┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ type             │ device                                        │
│ certificate_file │ ./testdevice.crt ✓                            │
│ key_file         │ ./testdevice.key.decrypt ✓                    │
│ lfdi             │ 0F3078CFDDAEE28DC20B95635DC116CC2A6D877F      │
│ sfdi             │ 40773583337                                   │
│ max_watts        │ 5000                                          │
│ pen              │ 12345                                         │
│ pin              │ 111115                                        │
│ user_agent       │ null                                          │
└──────────────────┴───────────────────────────────────────────────┘
```

To update individual client settings (eg to add a User-Agent header to requests) just specify the parameter to update and new value:

`cactus client myclient1 user_agent "cactus client myclient1"`

### Discovering available tests

The command `cactus tests` will print out all available test cases...

```
                                                     Available Test Procedures                                                     
┏━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Id       ┃ Category     ┃ Description                                                ┃ Required Clients                         ┃
┡━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ S-ALL-01 │ Registration │ Discovery with Out-Of-Band registration                    │ 1 client(s) with type(s): any            │
│ S-ALL-02 │ Registration │ Discovery with In-Band Registration for Direct Clients     │ 1 client(s) with type(s): device         │
...
```


### Running your first test

The following command will run the `S-ALL-01` test with the client you created earlier:

```
cactus run S-ALL-01 myclient1
```

#### `cactus run` options

| Flag | Description |
|------|-------------|
| `--headless` | Disable the terminal UI — logs are written to stderr instead. Useful for CI/scripted environments. |
| `--timeout SECONDS` | Abort and fail the test if it exceeds this many seconds. |
| `--strict` | Treat warnings as failures. The test will be marked FAIL if any warnings were emitted, even if all steps passed. |
| `-c PATH` | Override the config file location (defaults to `./.cactus.yaml` then `~/.cactus.yaml`). |

### Running all tests automatically

`cactus autorun` runs all (or a selected subset of) test procedures sequentially, assigning configured clients to each test automatically. It stops at the first failure.

```
cactus autorun
cactus autorun --include S-ALL-01 S-ALL-02
cactus autorun --include-file my-tests.txt --exclude S-DER-09
cactus autorun --strict --headless --timeout 120
```

#### `cactus autorun` options

| Flag | Description |
|------|-------------|
| `--include ID [ID ...]` | Only run these test procedure IDs (in the order given). |
| `--include-file PATH` | Path to a text file listing test IDs to run, one per line (`#` lines are treated as comments). Merged with `--include`. |
| `--exclude ID [ID ...]` | Skip these test procedure IDs. Applied after `--include`/`--include-file`. |
| `--timeout SECONDS` | Per-test timeout in seconds. A test that times out is marked as failed and the run stops. |
| `--strict` | Treat warnings as failures for every test in the run. |
| `--headless` | Disable the terminal UI for all tests. |
| `-c PATH` | Override the config file location. |

#### Persistent autorun config

All `autorun` options (except `--headless` and `-c`) can be stored in `.cactus.yaml` under the `runner` key so you don't need to pass them on every invocation. CLI flags always take precedence over the file.

```yaml
runner:
  include: []          # list of test IDs to include (empty = all)
  include_file: null   # path to an include-file
  exclude: []          # list of test IDs to skip
  timeout: null        # per-test timeout in seconds
  strict: false        # treat warnings as failures
```

### Viewing the compliance report

After running tests, print a summary of the latest result for each test procedure:

```
cactus report
```

This scans the configured `output_dir` and shows a table of every test procedure with its most recent PASS/FAIL result and timestamp. The same report is printed automatically at the end of `cactus autorun`.


### Admin plugins

Some test procedures include `admin_instruction` steps — directives for an out-of-band admin agent to perform setup on the server under test (e.g. registering end devices, issuing DER controls). These are handled by plugins loaded at runtime.

Plugins are built using [apluggy](https://github.com/nextline-dev/apluggy), a simple async wrapper around pytest's [pluggy](https://github.com/pytest-dev/pluggy). See [`plugins.py`](src/cactus_client/admin/plugins.py) for the hookspecs your plugin can implement:

| Hook | When called |
|------|-------------|
| `admin_setup` | Once before any test steps run |
| `admin_teardown` | Once after all steps complete (or on failure) — always runs |
| `admin_instruction` | Once per admin instruction, before the first attempt of the owning step |

Plugins are discovered automatically via setuptools entry points — no code changes to `cactus-client` required:

```toml
# pyproject.toml
[project.entry-points."cactus_client.admin"]
my-plugin = "my_package.plugin:MyServerPlugin"
```

Install your plugin alongside `cactus-client` (`pip install -e .`) and it will be loaded on next invocation.

**Reference implementation:** [cactus-client-envoy](https://github.com/bsgip/cactus-client-envoy) is a full worked example — it implements all three hooks against a local [Envoy](https://github.com/bsgip/envoy) CSIP-Aus server via direct database access, and includes setup scripts and a complete quickstart guide.