[![GitHub release](https://img.shields.io/github/release/PatrickBaus/sensorDaemon.svg)](../../releases/latest)
[![CI workflow](https://img.shields.io/github/actions/workflow/status/PatrickBaus/sensorDaemon/ci.yml?branch=master&label=ci&logo=github)](../../actions?workflow=ci)
[![code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=flat&logo=docker&logoColor=white)](../../pkgs/container/sensordaemon)
# LabKraken Sensor Daemon
`LabKraken` is a logging daemon for lab applications. It can extract data from a number of sources commonly found in
laboratories like:
 * [Tinkerforge](https://www.tinkerforge.com/en/shop/bricklets.html) sensors
 * [Labnodes](https://github.com/TU-Darmstadt-APQ/Labnode_PID)
 * SCPI enabled devices via GPIB or Ethernet

The data is extracted from the devices and published via [MQTT](https://en.wikipedia.org/wiki/MQTT) for further
processing. Used with a datalogger like the [Kraken Logger](https://github.com/PatrickBaus/database_logger) and
a data visualisation frontend like [Grafana](https://grafana.com/grafana/) it allows to precisely monitor labs and
experiments in real-time.

`LabKraken` is written in Python using the [asyncio](https://docs.python.org/3/library/asyncio.html) framework and runs on
both large scale servers and small [Raspberry Pis](https://www.raspberrypi.com/).

# Setup
`LabKraken` is best installed via the Docker repository provided with this repository.

## ... via [`docker-compose`](https://github.com/docker/compose)

Basic example `docker-compose.yml` for the `LabKraken`:
```yaml
services:
  kraken:
    image: ghcr.io/patrickbaus/sensordaemon:latest
    container_name: kraken
    restart: always
    environment:
      - SENSORS_DATABASE_HOST=mongodb://foo:bar@database-server:27017
      - MQTT_HOST=mqtt-broker
      - NODE_ID=a4777632-3de5-4682-a0a7-3f86d879c74d 
```

## Versioning
I use [SemVer](http://semver.org/) for versioning. For the versions available, see the
[tags on this repository](/../../tags).

## Documentation
I use the [Numpydoc](https://numpydoc.readthedocs.io/en/latest/format.html) style for documentation.

## Authors
* **Patrick Baus** - *Initial work* - [PatrickBaus](https://github.com/PatrickBaus)

## License
This project is licensed under the GPL v3 license - see the [LICENSE](LICENSE) file for details.
