[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Big Lots Cisco Call Manager Pack

# <a name="Introduction"></a> Introduction
This pack holds all of the actions and workflows for automated processes of Cisco Call Manager for Big Lots.

# <a name="QuickStart"></a> Quick Start

**Steps**

1. Install the pack

    ``` shell
    st2 pack install https://bitbucket.org/encoretech/stackstorm-biglots_call_manager.git
    ```

# <a name="Configuration"></a> Configuration

The configuration for this pack is used to specify connection information for both the SQL database and the MQ connections. The location for the config file is `/opt/stackstorm/configs/biglots_call_manager.yaml`.

**Note** : `st2 pack config` doesn't handle schemas references very well (known bug)
    so it's best to create the configuration file yourself and copy it into
    `/opt/stackstorm/configs/biglots_call_manager.yaml` then run `st2ctl reload --register-configs`

## <a name="Schema"></a> Schema

yaml for Schema would go below.


```
---
biglots_mq_sensor:
  mq_username: "{{ st2kv.system.<name_of_key> }}"
  mq_password: "{{ st2kv.system.<name_of_key> }}"
  qmgr_name: "{{ st2kv.system.<name_of_key> }}"
  mq_host: "{{ st2kv.system.<name_of_key> }}"
  mq_port: "{{ st2kv.system.<name_of_key> }}"
  mq_queue_name: "{{ st2kv.system.<name_of_key> }}"
  mq_channel_name: "{{ st2kv.system.<name_of_key> }}"
  db_sql_server: "{{ st2kv.system.<name_of_key> }}"
  db_sql_login: "{{ st2kv.system.<name_of_key> }}"
  db_sql_password: "{{ st2kv.system.<name_of_key> }}"
  db_sql_db: "{{ st2kv.<name_of_key> }}"
  biglots_api_key: "{{ st2kv.system.<name_of_key> }}"
```

# <a name="Dependancies"></a> Dependancies
Stackstorm Pack Dependencies (must be hand installed):
- ffmpeg
- MQSeriesSDK

Python Dependencies (installed automatically via requirements.txt):
- gtts
- pydub
- pymqi
- pymssql
- xmltodict
- deep_translator

# <a name="Installation"></a> Installation
Pack is installed via privately hosted bitbucket.

``` shell
st2 pack install https://bitbucket.org/encoretech/stackstorm-biglots_call_manager.git
```

# <a name="Actions"></a> Actions

Actions in this pack are used to call related workflows that are used in processes for automation of Big Lots Cisco Call Manager.

Actions:

|  Action  |  Description  |
|---|---|
|  clusters_sync  |  Kicks off the store_hours_sync action to update all clusters with latest info from database  |
|  schedule_check  |  Checks if a matching schedule exists on the cluster given hours from the MQ  |
|  schedule_create  |  Creates a schedule on the specified clusters  |
|  schedules_sync  |  Syncs locals schedules with schedules that exist on all clusters  |
|  store_hours_sync  |  Kicks off the store_update action for a given cluster  |
|  store_update  |  Updates all stores on a cluster to match the hours in the database  |

# <a name="Rules"></a> Rules

Rules in this pack are used as automatic triggers on a schedule

Rules:

|  Rule  |  Description  |
|---|---|
|  sync_clusters  |  Syncs store hours on all clusters with database  |

# <a name="Usage"></a> Usage

## <a name="BasicUsage"></a> Basic Usage

All actions are called automatically by the polling sensor, which kicks off the `clusters_sync` workflow at the end of its run to update all clusters.
You can manually stop the sensor with `st2 sensor disable biglots_call_manager.BigLotsMQSensor` and you can manually start the sensor with
`/opt/stackstorm/st2/bin/st2sensorcontainer --config-file=/etc/st2/st2.conf --sensor-ref=biglots_call_manager.BigLotsMQSensor`. If you make a change to any action files and want the sensor to recognize those changes, ensure that you run the `st2ctl reload --register-all` command to register
those changes. If you would like to manually run actions outside of the sensor usage, you can use the command format seen below.

``` shell
st2 run biglots_call_manager.example example_param='value'
```
