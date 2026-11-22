# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/PatrickBaus/sensorDaemon/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                               |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|--------------------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| \_version.py                                       |        1 |        0 |        0 |        0 |    100% |           |
| async\_event\_bus.py                               |       51 |       20 |       12 |        1 |     57% |49-65, 78-81, 95, 108-109 |
| connections/\_\_init\_\_.py                        |        1 |        0 |        0 |        0 |    100% |           |
| connections/ip\_connection.py                      |      119 |       52 |       22 |        4 |     50% |13-14, 55, 65, 75, 85, 96, 131, 158, 169-174, 181, 188-195, 203, 209-212, 220-242, 286-292, 316-322 |
| data\_types.py                                     |       30 |        3 |        8 |        0 |     92% |26, 29, 61 |
| database/\_\_init\_\_.py                           |        0 |        0 |        0 |        0 |    100% |           |
| database/models.py                                 |       62 |        6 |        2 |        0 |     88% |33-35, 55-58 |
| databases.py                                       |      366 |      246 |      100 |       10 |     30% |14-15, 58-59, 64-65, 70, 76-117, 137, 144, 149, 159, 166, 168, 170, 172, 174, 196, 233, 236, 283-\>287, 303, 310-321, 332-333, 336-338, 343, 359-398, 410-427, 438-439, 442-444, 449, 465-507, 519-536, 547-548, 551-554, 559-560, 573-574, 590-629, 641-658, 669-670, 673-675, 680, 696-736, 748-767 |
| errors.py                                          |        3 |        0 |        0 |        0 |    100% |           |
| helper\_functions.py                               |      111 |        5 |       40 |        6 |     93% |73-\>76, 109-\>112, 188-\>190, 196-\>exit, 221-225, 227-\>exit, 255 |
| kraken\_ethernet.py                                |      125 |      125 |       20 |        0 |      0% |    25-288 |
| managers.py                                        |      203 |       36 |       46 |        3 |     83% |271-\>exit, 276, 297-\>300, 324-326, 333-346, 349, 371-385, 413-434, 534-551 |
| sensors/\_\_init\_\_.py                            |        0 |        0 |        0 |        0 |    100% |           |
| sensors/drivers/generic\_driver.py                 |       77 |        4 |       24 |        3 |     93% |44, 166, 173-188 |
| sensors/drivers/generic\_scpi\_driver.py           |      105 |        1 |       16 |        0 |     99% |       294 |
| sensors/drivers/hp3478a\_driver.py                 |       10 |        1 |        0 |        0 |     90% |        34 |
| sensors/drivers/labnode.py                         |       49 |       28 |        6 |        0 |     38% |32, 35-37, 41, 55-82, 85-94, 99-101, 110, 113-132 |
| sensors/drivers/shared.py                          |        3 |        0 |        0 |        0 |    100% |           |
| sensors/drivers/tinkerforge.py                     |       92 |       37 |        8 |        0 |     57% |14-15, 52, 70-71, 76, 86, 103, 126-168, 186-195, 200-210, 284-293, 305, 316-336 |
| sensors/factories/\_\_init\_\_.py                  |        0 |        0 |        0 |        0 |    100% |           |
| sensors/factories/sensor\_factory.py               |       20 |        6 |        0 |        0 |     70% | 38, 62-66 |
| sensors/transports/\_\_init\_\_.py                 |        0 |        0 |        0 |        0 |    100% |           |
| sensors/transports/ethernet.py                     |       13 |        2 |        0 |        0 |     85% |    36, 48 |
| sensors/transports/generic\_ethernet\_transport.py |        9 |        2 |        0 |        0 |     78% |     27-43 |
| sensors/transports/generic\_transport.py           |       42 |       21 |        2 |        0 |     48% |30, 40, 50, 60, 91-96, 100-111, 114-141 |
| sensors/transports/labnode.py                      |       20 |        4 |        0 |        0 |     80% |41, 53, 64-81 |
| sensors/transports/linux\_gpib.py                  |       20 |        4 |        0 |        0 |     80% |42, 55, 73-87 |
| sensors/transports/prologix\_ethernet.py           |       16 |        3 |        0 |        0 |     81% |38, 52, 65 |
| sensors/transports/tinkerforge.py                  |       54 |       25 |        8 |        0 |     47% |44, 54, 64, 69-71, 86-97, 109-111, 115-132, 141-151 |
| sensors/transports/transport\_factory.py           |       23 |        4 |        2 |        0 |     76% |     56-59 |
| **TOTAL**                                          | **1625** |  **635** |  **316** |   **27** | **59%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/PatrickBaus/sensorDaemon/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/PatrickBaus/sensorDaemon/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/PatrickBaus/sensorDaemon/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/PatrickBaus/sensorDaemon/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2FPatrickBaus%2FsensorDaemon%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/PatrickBaus/sensorDaemon/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.