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
└──────────────────┴───────────────────────────────────────────────┘
```

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

The following command will run the `S-ALL-01` test with the client you created earlier `cactus run S-ALL-01 myclient1`

