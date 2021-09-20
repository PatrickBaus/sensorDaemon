#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from beanie import init_beanie
from datetime import datetime
import logging
import pymongo
import motor
import warnings

from database.models import TinkerforgeSensor, GpibSensor, FunctionCall, SensorHost, SensorUnit, TinkforgeSensorConfig


class Sensor():
    """
    A sensor mock, that prints args and kwargs
    """
    @staticmethod
    def set_status_led_config(*args, **kwargs):
        print("Args:", args)
        print("Kwargs:", kwargs)


async def main():
    # Beanie uses Motor under the hood
    client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://root:example@server.lan:27017")
    #client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://root:example@hal43.apq:27017")
    print(client)

    await init_beanie(database=client.sensor_config, document_models=[SensorHost, SensorUnit, TinkerforgeSensor, GpibSensor])

    hosts = (
        ("10.0.0.5", 4223, "tinkerforge", "Server Monitor"),
        ("192.168.1.152", 4223, "tinkerforge", "Tinkerforge Test"),
        ("192.168.1.104", 1234, "prologix_gpib", "GPIB Test"),
        ("127.0.0.1", 1234, "prologix_gpib", "GPIB Test"),
    )
    for hostname, port, driver, label in hosts:
        host = SensorHost(
            hostname=hostname,
            port=port,
            driver=driver,
            label=label
        )
        try:
            await host.save()
        except pymongo.errors.OperationFailure:
            host = await SensorHost.find_one(SensorHost.hostname == "10.0.0.5", SensorHost.port == 4223)
        print(host)

    unit_kelvin = await SensorUnit.find_one(SensorUnit.label == "K")
    if not unit_kelvin:
        unit_kelvin = SensorUnit(label="K")
        await unit_kelvin.save()
    # print(unit_kelvin)

    sensor_config1 = TinkforgeSensorConfig(
        interval=2000,
        trigger_only_on_change=False,
        label="Server Temperature",
        unit=unit_kelvin.id,
    )

    sensor_config2 = TinkforgeSensorConfig(
        interval=1000,
        trigger_only_on_change=False,
        label="Server Humidity",
        unit=unit_kelvin.id,
    )

    sensor = TinkerforgeSensor(
        uid=125633,
        config={
            0: sensor_config1,
            1: sensor_config2,
        },
        on_connect=[
            FunctionCall(function="set_status_led_config", kwargs={"config": 2}),
        ]
    )
    try:
        await sensor.save()
    except pymongo.errors.OperationFailure:
        pass

    sensor_config1 = TinkforgeSensorConfig(
        interval=1000,
        trigger_only_on_change=False,
        label="Test Voltage",
        unit=unit_kelvin.id,
    )
    sensor = TinkerforgeSensor(
        uid=169087,
        config={
            0: sensor_config1,
        },
        on_connect=[
            FunctionCall(function="set_status_led_config", kwargs={"config": 2}),
        ]
    )
    try:
        await sensor.save()
    except pymongo.errors.OperationFailure:
        pass

    gpib_host = await SensorHost.find_one(SensorHost.hostname == "127.0.0.1", SensorHost.port == 1234)
    sensor = GpibSensor(
        uid="HP3478A_2520A20614",
        pad=27,
        driver='hp3478a',
        label="HP 3478A Test",
        unit=unit_kelvin.id,
        interval=1000,
        host=gpib_host.id,
        on_connect=[
            FunctionCall(function="set_function", args=[9, ]),
            FunctionCall(function="set_range", args=[4, ]),
            FunctionCall(function="set_trigger", args=[1, ]),
            FunctionCall(function="set_autozero", args=[True, ]),
            FunctionCall(function="set_number_of_digits", args=[6, ]),
            FunctionCall(function="set_ntc_parameters", kwargs={'a': 3.3540154E-03, 'b': 2.5627725E-04, 'c': 2.0829210E-06, 'd': 7.3003206E-08, 'rt25': 10000}),
        ],
        before_read=[],
        after_read=[]
    )
    try:
        await sensor.save()
    except pymongo.errors.OperationFailure:
        pass

#    sensor4 = TinkerforgeSensor(
#        uid=169087,
#        label="Test Sensor",
#        unit=unit_kelvin.id,
#        interval=1000,
#        on_connect=[],
#    )

#    try:
#        await sensor2.save()
#    except pymongo.errors.OperationFailure as e:
#        pass

#    try:
#        await sensor4.save()
#    except pymongo.errors.OperationFailure as e:
#        pass

    # You can find documents with pythonic syntax
    tf_sensor = await TinkerforgeSensor.find_one(TinkerforgeSensor.uid == 169087)
    print("Found TF sensor:", tf_sensor)

    if tf_sensor is not None:
        sens = Sensor()
        for function_call in tf_sensor.on_connect:
            function_call.execute(sens)
        # And update them
        await tf_sensor.set({TinkerforgeSensor.date_modified: datetime.utcnow()})

    tf_sensor = await TinkerforgeSensor.find_one(TinkerforgeSensor.uid == 169087)
    print("Found modified TF sensor:", tf_sensor)

    gpib_sensor = await GpibSensor.find_one(GpibSensor.pad == 22)
    print("Found GPIB sensor:", gpib_sensor)

    async for sensor in TinkerforgeSensor.find_all():
        print("All sensors", sensor)

    #await asyncio.sleep(2)
    #await host.delete()

    host = SensorHost(
        hostname="192.168.1.152",
        port=4223,
        driver='tinkerforge',
        label="TF Test"
    )
    #try:
    #    await host.save()
    #except pymongo.errors.OperationFailure as e:
    #    host = await SensorHost.find_one(SensorHost.hostname == "192.168.1.152", SensorHost.port == 4223)

# Report all mistakes managing asynchronous resources.
warnings.simplefilter('always', ResourceWarning)
logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    level=logging.INFO,    # Enable logs from the ip connection. Set to debug for even more info
    datefmt='%Y-%m-%d %H:%M:%S'
)

asyncio.run(main(), debug=True)
