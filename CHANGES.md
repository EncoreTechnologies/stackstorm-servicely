# Change Log

## v2.0.0

Adds C_parent field to top level of response payloads.

## v1.0.8

Updates pack config to handle list of connections to poll multiple Servicely instances at once.
Updates actions to handle new pack config and params.
Adds StackStorm hostname and Servicely Queue Name as top level fields in response payload.

## v1.0.7

Removes environment-specific servicely token key name and associated logic.

## v1.0.6

Updates pack config to use dynamic queue endpoint and updates all action files accordingly.

## v1.0.5

Added error handling that gets sent back to Servicely for async tasks.

## v1.0.4

Added ability to override the Servicely instance that results are being delivered to. Did specific token look ups for added security.
Updated actions get method to send data directly to Servicely.

## v1.0.3

Fixes group of exec params to be Watchman-specific to not interfere with other actions.

## v1.0.2

Added new actions to get all available actions and all the information needed to run those actions from Servicely.

## v1.0.1

Added three new actions to handle integration with Watchman for groups, computers, and alerts.

## v1.0.0

Added two sensors and triggers that monitor the queue and execution sync and async actions after they are published on the Servicely Queue.
