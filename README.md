# Lab Data Logger ‒ A (distributed) CLI data logger for the (physics) lab.
[![PyPI](https://img.shields.io/pypi/v/lab_data_logger?color=blue)](https://pypi.org/project/lab_data_logger/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A command-line tool that allows logging of data locally or over a network to a InfluxDB. 

## Installation

```
pip install lab_data_logger
```

## Usage
### Basic usage

Lab Data Logger comes with a CLI tool, `ldl`. To get help with all the available options,
use `ldl --help`.

In these examples 
#### Setting up a data source

First, we setup a data service that provides data to be logged. As an example, we use 
the `RandomNumerService` provided. Here, we want to 

```
$ ldl services start lab_data_logger.services.RandomNumberService 18862 
Trying to start RandomNumberService from lab_data_logger.services
Started RandomNumberService on port 18862.
```
 
#### Setting up the logger
Second, in order to log te data provided by this service, we first start a logger. We 
want the Logger to be accessible at port 18866. We pass the default host port and database
 of the InfluxDB explicitly for demonstration:


```
$ ldl logger --port 18866 start --host localhost --port 8083 --database test
Started logger on port 18866.
```

Open another terminal to add the data service to the logger:
```
ldl logger --port 18866 add --interval 3 localhost:18862 rand_num1
```
The second argument `rand_num1` is the [measurement](https://docs.influxdata.com/influxdb/v2.0/reference/key-concepts/data-elements/#measurement) the data is written to. The `--interval` option specifies the logging interval (in this case 3 seconds).

If succesful, the terminal where the logger service was started wil print `Connected to RANDOMNUMBER on port 18862`

#### Show logger status
```
$ ldl logger --port 18866 show
LAB DATA LOGGER
Logging to test on localhost:8083 (processed entry 66).
Pulling from these services:
MEASUREMENT   |     HOSTNAME     |    PORT    |   COUNTER   
-----------   |   ------------   |   ------   |   -------   
rand_num1     |   localhost      |    18862   |        66
```
### Creating your own data services

To make use of LDL, you have to create your own data services by subclassing `lab_data_services.services.LabDataService`. An example is given in the examples folder.

To start this data service, use the relative path from your current location, for example
from the parent directory of the cloned git repo:

```
    $ ldl service run lab_data_logger.examples.const_numbers.ConstNumberService
    Trying to start ConstNumberService from lab_data_logger.examples.const_numbers
    No module lab_data_logger.examples.const_numbers found
    Looking for ConstNumberService in ~/lab_data_logger/examples/const_numbers.py
    Started ConstNumberService on port 18861.
```

## Authors

-   Bastian Leykauf (<https://github.com/bleykauf>)

## License
Lab Data Logger ‒ A (distributed) CLI data logger for the (physics) lab.

Copyright © 2020 Bastian Leykauf

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see https://www.gnu.org/licenses/.
